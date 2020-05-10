package bottaio.s3upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

import static com.amazonaws.services.s3.internal.Constants.MB;

public class StreamTransferManager {

    @Data
    @Builder
    public static class Config {
        @NonNull
        private final String bucketName;
        @NonNull
        private final String putKey;

        @Builder.Default
        private int numStreams = 1;
        @Builder.Default
        private int numUploadThreads = 1;
        @Builder.Default
        private int queueCapacity = 1;
        @Builder.Default
        private int partSize = 5;
        @Builder.Default
        private boolean checkIntegrity = false;
    }

    // todo: replace it
    private void validateConfig(Config config) {
        if (config.checkIntegrity) {
            Utils.md5();  // check that algorithm is available
        }

        if (config.numStreams < 1) {
            throw new IllegalArgumentException("There must be at least one stream");
        }

        if (config.numUploadThreads < 1) {
            throw new IllegalArgumentException("There must be at least one upload thread");
        }

        if (config.queueCapacity < 1) {
            throw new IllegalArgumentException("The queue capacity must be at least 1");
        }

        if (config.partSize * MB < MultiPartOutputStream.S3_MIN_PART_SIZE) {
            throw new IllegalArgumentException(String.format(
                "The given part size (%d) is less than 5 MB.", config.partSize));
        }

        if (((long) config.partSize) * MB > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                "The given part size (%d) is too large as it does not fit in a 32 bit int", config.partSize));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);

    protected String uploadId;
    private final Config config;
    private final AmazonS3 s3Client;
    private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());
    private List<MultiPartOutputStream> multiPartOutputStreams;
    private ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
    private BlockingQueue<StreamPart> queue;
    private int finishedCount = 0;
    private StreamPart leftoverStreamPart = null;
    private final Object leftoverStreamPartLock = new Object();
    private boolean isAborting = false;
    private static final int MAX_PART_NUMBER = 10000;

    public StreamTransferManager(Config config, AmazonS3 s3Client) {
        validateConfig(config);
        this.config = config;
        this.s3Client = s3Client;
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

        queue = new ArrayBlockingQueue<StreamPart>(config.queueCapacity);
        log.debug("Initiating multipart upload to {}/{}", config.bucketName, config.putKey);
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(config.bucketName, config.putKey);
        customiseInitiateRequest(initRequest);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        uploadId = initResponse.getUploadId();
        log.info("Initiated multipart upload to {}/{} with full ID {}", config.bucketName, config.putKey, uploadId);
        try {
            multiPartOutputStreams = new ArrayList<MultiPartOutputStream>();
            ExecutorService threadPool = Executors.newFixedThreadPool(config.numUploadThreads);

            int partNumberStart = 1;

            for (int i = 0; i < config.numStreams; i++) {
                int partNumberEnd = (i + 1) * MAX_PART_NUMBER / config.numStreams + 1;
                MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd, config.partSize * MB, queue);
                partNumberStart = partNumberEnd;
                multiPartOutputStreams.add(multiPartOutputStream);
            }

            executorServiceResultsHandler = new ExecutorServiceResultsHandler<Void>(threadPool);
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
                ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[]{});
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(0);
                PutObjectRequest request = new PutObjectRequest(config.bucketName, config.putKey, emptyStream, metadata);
                customisePutEmptyObjectRequest(request);
                s3Client.putObject(request);
            } else {
                CompleteMultipartUploadRequest completeRequest = new
                        CompleteMultipartUploadRequest(
                        config.bucketName,
                        config.putKey,
                        uploadId,
                        partETags);
                customiseCompleteRequest(completeRequest);
                CompleteMultipartUploadResult completeMultipartUploadResult = s3Client.completeMultipartUpload(completeRequest);
                if (config.checkIntegrity) {
                    checkCompleteFileIntegrity(completeMultipartUploadResult.getETag());
                }
            }
            log.info("{}: Completed", this);
        } catch (IntegrityCheckException e) {
            // Nothing to abort. Upload has already finished.
            throw e;
        } catch (Throwable e) {
            throw abort(e);
        }
    }

    private void checkCompleteFileIntegrity(String s3ObjectETag) {
        List<PartETag> parts = new ArrayList<PartETag>(partETags);
        parts.sort(Comparator.comparing(PartETag::getPartNumber));
        String expectedETag = computeCompleteFileETag(parts);
        if (!expectedETag.equals(s3ObjectETag)) {
            throw new IntegrityCheckException(String.format(
                    "File upload completed, but integrity check failed. Expected ETag: %s but actual is %s",
                    expectedETag, s3ObjectETag));
        }
    }

    private String computeCompleteFileETag(List<PartETag> parts) {
        // When S3 combines the parts of a multipart upload into the final object, the ETag value is set to the
        // hex-encoded MD5 hash of the concatenated binary-encoded (raw bytes) MD5 hashes of each part followed by
        // "-" and the number of parts.
        MessageDigest md = Utils.md5();
        for (PartETag partETag : parts) {
            md.update(BinaryUtils.fromHex(partETag.getETag()));
        }
        // Represent byte array as a 32-digit number hexadecimal format followed by "-<partCount>".
        return String.format("%032x-%d", new BigInteger(1, md.digest()), parts.size());
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
            AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                    config.bucketName, config.putKey, uploadId);
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            log.info("{}: Aborted", this);
        }
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

    private void uploadStreamPart(StreamPart part) {
        log.debug("{}: Uploading {}", this, part);

        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(config.bucketName).withKey(config.putKey)
                .withUploadId(uploadId).withPartNumber(part.getPartNumber())
                .withInputStream(part.getInputStream())
                .withPartSize(part.size());
        if (config.checkIntegrity) {
            uploadRequest.setMd5Digest(part.getMD5Digest());
        }
        customiseUploadPartRequest(uploadRequest);

        UploadPartResult uploadPartResult = s3Client.uploadPart(uploadRequest);
        PartETag partETag = uploadPartResult.getPartETag();
        partETags.add(partETag);
        log.info("{}: Finished uploading {}", this, part);
    }

    @Override
    public String toString() {
        return String.format("[Manager uploading to %s/%s with id %s]",
                config.bucketName, config.putKey, Utils.skipMiddle(uploadId, 21));
    }

    // These methods are intended to be overridden for more specific interactions with the AWS API.

    @SuppressWarnings("unused")
    public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
    }

    @SuppressWarnings("unused")
    public void customiseUploadPartRequest(UploadPartRequest request) {
    }

    @SuppressWarnings("unused")
    public void customiseCompleteRequest(CompleteMultipartUploadRequest request) {
    }

    @SuppressWarnings("unused")
    public void customisePutEmptyObjectRequest(PutObjectRequest request) {
    }
}