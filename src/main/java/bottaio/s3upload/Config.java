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
  private long partSize = 5;
  @Builder.Default
  private boolean checkIntegrity = false;

  public StreamTransferManagerConfig toStreamTransferManagerConfig() {
    if (checkIntegrity) {
      Utils.md5();  // check that algorithm is available
    }

    if (partSize * MB < MultipartOutputStream.S3_MIN_PART_SIZE) {
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
        .partSize((int) (partSize * MB))
        .checkIntegrity(checkIntegrity)
        .build();
  }
}
