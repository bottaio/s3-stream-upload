package me.bottaio.streamupload;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static com.amazonaws.services.s3.internal.Constants.MB;

@Slf4j
public class StreamTransferManager {

  private final Config config;
  private final StreamPartUploader uploader;

  @Getter
  private final MultipartOutputStream multiPartOutputStream;

  public StreamTransferManager(Config config, StreamPartUploader uploader) {
    this.config = config;
    this.uploader = uploader;

    log.debug("Initiating multipart upload to {}/{}", this.config.bucketName, this.config.putKey);
    uploader.initialize();
    log.info("Initiated multipart upload to {}/{}", this.config.bucketName, this.config.putKey);

    try {
      this.multiPartOutputStream = new MultipartOutputStream(this.config.partSize, uploader);
    } catch (Throwable e) {
      throw abort(e);
    }
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
    log.error("Aborting {} due to error: {}", this, t.toString());
    uploader.abort();

    if (t instanceof Error) {
      throw (Error) t;
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else {
      throw new RuntimeException(t);
    }
  }

  @Override
  public String toString() {
    return String.format("[Manager uploading to %s/%s]", config.bucketName, config.putKey);
  }

  @Builder
  @Data
  public static class Config {
    @NonNull
    private final String bucketName;
    @NonNull
    private final String putKey;

    @Builder.Default
    private final int partSize = 5 * MB;
    @Builder.Default
    private final boolean checkIntegrity = false;
  }
}