package bottaio.s3upload;

import bottaio.s3upload.StreamTransferManager.StreamTransferManagerConfig;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import static com.amazonaws.services.s3.internal.Constants.MB;

@Data
@Builder
public class Config {
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
  private long partSize = 5;
  @Builder.Default
  private boolean checkIntegrity = false;

  public StreamTransferManagerConfig toStreamTransferManagerConfig() {
    if (checkIntegrity) {
      Utils.md5();  // check that algorithm is available
    }

    if (numStreams < 1) {
      throw new IllegalArgumentException("There must be at least one stream");
    }

    if (numUploadThreads < 1) {
      throw new IllegalArgumentException("There must be at least one upload thread");
    }

    if (queueCapacity < 1) {
      throw new IllegalArgumentException("The queue capacity must be at least 1");
    }

    if (partSize * MB < MultiPartOutputStream.S3_MIN_PART_SIZE) {
      throw new IllegalArgumentException(String.format(
          "The given part size (%d) is less than 5 MB.", partSize * MB));
    }

    if (partSize * MB > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(String.format(
          "The given part size (%d) is too large as it does not fit in a 32 bit int", partSize * MB));
    }

    return StreamTransferManagerConfig.builder()
        .bucketName(bucketName)
        .putKey(putKey)
        .numStreams(numStreams)
        .numUploadThreads(numUploadThreads)
        .queueCapacity(queueCapacity)
        .partSize((int) (partSize * MB))
        .checkIntegrity(checkIntegrity)
        .build();
  }
}
