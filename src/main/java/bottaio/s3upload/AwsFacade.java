package bottaio.s3upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import lombok.AllArgsConstructor;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@AllArgsConstructor
public class AwsFacade {
  private final Config config;
  private final AmazonS3 s3Client;

  protected InitiateMultipartUploadRequest initiateMultipartUploadRequest() {
    return new InitiateMultipartUploadRequest(config.getBucketName(), config.getPutKey());
  }

  protected UploadPartRequest uploadPartRequest(String uploadId, StreamPart part) {
    return new UploadPartRequest()
        .withBucketName(config.getBucketName()).withKey(config.getPutKey())
        .withUploadId(uploadId)
        .withPartNumber(part.getPartNumber())
        .withInputStream(part.getInputStream())
        .withPartSize(part.size());
  }

  protected PutObjectRequest putEmptyObjectRequest() {
    ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);
    return new PutObjectRequest(config.getBucketName(), config.getPutKey(), emptyStream, metadata);
  }

  protected CompleteMultipartUploadRequest finalizeUploadRequest(String uploadId, List<PartETag> partETags) {
    return new CompleteMultipartUploadRequest(config.getBucketName(), config.getPutKey(), uploadId, partETags);
  }

  protected AbortMultipartUploadRequest abortUploadRequest(String uploadId) {
    return new AbortMultipartUploadRequest(config.getBucketName(), config.getPutKey(), uploadId);
  }

  public final String initializeUpload() {
    InitiateMultipartUploadRequest initRequest = initiateMultipartUploadRequest();
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    return initResponse.getUploadId();
  }

  public final PartETag uploadPart(String uploadId, StreamPart part) {
    UploadPartRequest uploadRequest = uploadPartRequest(uploadId, part);
    if (config.isCheckIntegrity()) {
      uploadRequest.setMd5Digest(part.getMD5Digest());
    }

    UploadPartResult uploadPartResult = s3Client.uploadPart(uploadRequest);
    return uploadPartResult.getPartETag();
  }

  public final void putEmptyObject() {
    PutObjectRequest request = putEmptyObjectRequest();
    s3Client.putObject(request);
  }

  public final void finalizeUpload(String uploadId, List<PartETag> partETags) {
    CompleteMultipartUploadRequest completeRequest = finalizeUploadRequest(uploadId, partETags);
    CompleteMultipartUploadResult completeMultipartUploadResult = s3Client.completeMultipartUpload(completeRequest);
    if (config.isCheckIntegrity()) {
      checkCompleteFileIntegrity(completeMultipartUploadResult.getETag(), partETags);
    }
  }

  public final void abortUpload(String uploadId) {
    AbortMultipartUploadRequest abortMultipartUploadRequest = abortUploadRequest(uploadId);
    s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }

  // todo: move it
  private void checkCompleteFileIntegrity(String s3ObjectETag, List<PartETag> partETags) {
    List<PartETag> parts = new ArrayList<>(partETags);
    parts.sort(Comparator.comparing(PartETag::getPartNumber));
    String expectedETag = computeCompleteFileETag(parts);
    if (!expectedETag.equals(s3ObjectETag)) {
      throw new IntegrityCheckException(String.format(
          "File upload completed, but integrity check failed. Expected ETag: %s but actual is %s",
          expectedETag, s3ObjectETag));
    }
  }

  // todo: move it
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
}
