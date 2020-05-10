package bottaio.s3upload;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamTransferManager {

  private final StreamTransferManagerConfig config;
  private final StreamPartUploader uploader;
  private MultipartOutputStream multiPartOutputStream;
  private boolean isAborting = false;

  public StreamTransferManager(Config config, StreamPartUploader uploader) {
    this.config = config.toStreamTransferManagerConfig();
    this.uploader = uploader;
  }

  /**
   * Get the list of output streams to write to.
   * <p>
   * The first call to this method initiates the multipart upload.
   * All setter methods must be called before this.
   */
  public MultipartOutputStream getMultiPartOutputStream() {
    if (multiPartOutputStream != null) {
      return multiPartOutputStream;
    }

    log.debug("Initiating multipart upload to {}/{}", config.bucketName, config.putKey);
    uploader.initialize();
    log.info("Initiated multipart upload to {}/{} with full ID {}", config.bucketName, config.putKey, uploader.getUploadId());

    try {
      multiPartOutputStream = new MultipartOutputStream(config.partSize, uploader);
    } catch (Throwable e) {
      throw abort(e);
    }

    return multiPartOutputStream;
  }

  /**
   * Blocks while waiting for the threads uploading the contents of the streams returned
   * by {@link StreamTransferManager#getMultiPartOutputStream()} to finish, then sends a request to S3 to complete
   * the upload. For the former to complete, it's essential that every stream is closed, otherwise the upload
   * threads will block forever waiting for more data.
   */
  public void complete() {
    try {
      uploader.complete();
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
    uploader.abort();
  }

  @Override
  public String toString() {
    return String.format("[Manager uploading to %s/%s with id %s]",
        config.bucketName, config.putKey, Utils.skipMiddle(uploader.getUploadId(), 21));
  }

  @Builder
  public static class StreamTransferManagerConfig {
    private final String bucketName;
    private final String putKey;
    private final int partSize;
    private final boolean checkIntegrity;
  }
}