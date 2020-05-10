package bottaio.s3upload;

import com.amazonaws.services.s3.model.PartETag;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class StreamTransferManager {

  private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);
  private static final int MAX_PART_NUMBER = 10000;
  private final StreamTransferManagerConfig config;
  private final AwsFacade awsFacade;
  private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());
  private final Object leftoverStreamPartLock = new Object();
  private String uploadId;
  private List<MultiPartOutputStream> multiPartOutputStreams;
  private ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
  private BlockingQueue<StreamPart> queue;
  private int finishedCount = 0;
  private StreamPart leftoverStreamPart = null;
  private boolean isAborting = false;

  public StreamTransferManager(Config config, AwsFacade awsFacade) {
    this.config = config.toStreamTransferManagerConfig();
    this.awsFacade = awsFacade;
  }

  /**
   * Get the list of output streams to write to.
   * <p>
   * The first call to this method initiates the multipart upload.
   * All setter methods must be called before this.
   */
  public List<MultiPartOutputStream> getMultiPartOutputStreams() {
    if (multiPartOutputStreams != null) {
      return multiPartOutputStreams;
    }

    queue = new ArrayBlockingQueue<>(config.queueCapacity);
    log.debug("Initiating multipart upload to {}/{}", config.bucketName, config.putKey);
    uploadId = awsFacade.initializeUpload();
    log.info("Initiated multipart upload to {}/{} with full ID {}", config.bucketName, config.putKey, uploadId);
    try {
      multiPartOutputStreams = new ArrayList<>();
      ExecutorService threadPool = Executors.newFixedThreadPool(config.numUploadThreads);

      int partNumberStart = 1;

      for (int i = 0; i < config.numStreams; i++) {
        int partNumberEnd = (i + 1) * MAX_PART_NUMBER / config.numStreams + 1;
        MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd, config.partSize, queue);
        partNumberStart = partNumberEnd;
        multiPartOutputStreams.add(multiPartOutputStream);
      }

      executorServiceResultsHandler = new ExecutorServiceResultsHandler<>(threadPool);
      for (int i = 0; i < config.numUploadThreads; i++) {
        executorServiceResultsHandler.submit(new UploadTask());
      }
      executorServiceResultsHandler.finishedSubmitting();
    } catch (Throwable e) {
      throw abort(e);
    }

    return multiPartOutputStreams;
  }

  /**
   * Blocks while waiting for the threads uploading the contents of the streams returned
   * by {@link StreamTransferManager#getMultiPartOutputStreams()} to finish, then sends a request to S3 to complete
   * the upload. For the former to complete, it's essential that every stream is closed, otherwise the upload
   * threads will block forever waiting for more data.
   */
  public void complete() {
    try {
      log.debug("{}: Waiting for pool termination", this);
      executorServiceResultsHandler.awaitCompletion();
      log.debug("{}: Pool terminated", this);
      if (leftoverStreamPart != null) {
        log.info("{}: Uploading leftover stream {}", this, leftoverStreamPart);
        uploadStreamPart(leftoverStreamPart);
        log.debug("{}: Leftover uploaded", this);
      }
      log.debug("{}: Completing", this);
      if (partETags.isEmpty()) {
        log.debug("{}: Uploading empty stream", this);
        awsFacade.putEmptyObject();
      } else {
        awsFacade.finalizeUpload(uploadId, partETags);
      }
      log.info("{}: Completed", this);
    } catch (IntegrityCheckException e) {
      // Nothing to abort. Upload has already finished.
      throw e;
    } catch (Throwable e) {
      throw abort(e);
    }
  }

  /**
   * Aborts the upload and rethrows the argument, wrapped in a RuntimeException if necessary.
   * Write {@code throw abort(e)} to make it clear to the compiler and readers that the code
   * stops here.
   */
  public RuntimeException abort(Throwable t) {
    if (!isAborting) {
      log.error("Aborting {} due to error: {}", this, t.toString());
    }
    abort();
    if (t instanceof Error) {
      throw (Error) t;

    } else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;

    } else if (t instanceof InterruptedException) {
      throw Utils.runtimeInterruptedException((InterruptedException) t);

    } else {
      throw new RuntimeException(t);
    }
  }

  /**
   * Aborts the upload. Repeated calls have no effect.
   */
  public void abort() {
    synchronized (this) {
      if (isAborting) {
        return;
      }
      isAborting = true;
    }
    if (executorServiceResultsHandler != null) {
      executorServiceResultsHandler.abort();
    }
    if (uploadId != null) {
      log.debug("{}: Aborting", this);
      awsFacade.abortUpload(uploadId);
      log.info("{}: Aborted", this);
    }
  }

  private void uploadStreamPart(StreamPart part) {
    log.debug("{}: Uploading {}", this, part);
    PartETag partETag = awsFacade.uploadPart(uploadId, part);
    partETags.add(partETag);
    log.info("{}: Finished uploading {}", this, part);
  }

  @Override
  public String toString() {
    return String.format("[Manager uploading to %s/%s with id %s]",
        config.bucketName, config.putKey, Utils.skipMiddle(uploadId, 21));
  }

  @Builder
  public static class StreamTransferManagerConfig {
    private final String bucketName;
    private final String putKey;
    private final int numStreams;
    private final int numUploadThreads;
    private final int queueCapacity;
    private final int partSize;
    private final boolean checkIntegrity;
  }

  private class UploadTask implements Callable<Void> {

    @Override
    public Void call() {
      try {
        while (true) {
          StreamPart part;
          //noinspection SynchronizeOnNonFinalField
          synchronized (queue) {
            if (finishedCount < multiPartOutputStreams.size()) {
              part = queue.take();
              if (part == StreamPart.POISON) {
                finishedCount++;
                continue;
              }
            } else {
              break;
            }
          }
          if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
                    /*
                    Each stream does its best to avoid producing parts smaller than 5 MB, but if a user doesn't
                    write that much data there's nothing that can be done. These are considered 'leftover' parts,
                    and must be merged with other leftovers to try producing a part bigger than 5 MB which can be
                    uploaded without problems. After the threads have completed there may be at most one leftover
                    part remaining, which S3 can accept. It is uploaded in the complete() method.
                    */
            log.debug("{}: Received part {} < 5 MB that needs to be handled as 'leftover'", this, part);
            StreamPart originalPart = part;
            part = null;
            synchronized (leftoverStreamPartLock) {
              if (leftoverStreamPart == null) {
                leftoverStreamPart = originalPart;
                log.debug("{}: Created new leftover part {}", this, leftoverStreamPart);
              } else {
                                /*
                                Try to preserve order within the data by appending the part with the higher number
                                to the part with the lower number. This is not meant to produce a perfect solution:
                                if the client is producing multiple leftover parts all bets are off on order.
                                */
                if (leftoverStreamPart.getPartNumber() > originalPart.getPartNumber()) {
                  StreamPart temp = originalPart;
                  originalPart = leftoverStreamPart;
                  leftoverStreamPart = temp;
                }
                leftoverStreamPart.getOutputStream().append(originalPart.getOutputStream());
                log.debug("{}: Merged with existing leftover part to create {}", this, leftoverStreamPart);
                if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
                  log.debug("{}: Leftover part can now be uploaded as normal and reset", this);
                  part = leftoverStreamPart;
                  leftoverStreamPart = null;
                }
              }
            }
          }
          if (part != null) {
            uploadStreamPart(part);
          }
        }
      } catch (Throwable t) {
        throw abort(t);
      }

      return null;
    }

  }
}