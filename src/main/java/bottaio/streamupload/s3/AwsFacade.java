package bottaio.streamupload.s3;

import bottaio.streamupload.StreamPart;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static bottaio.streamupload.StreamTransferManager.Config;

@RequiredArgsConstructor
public class AwsFacade {
  private final Config config;
  private final AmazonS3 s3Client;
  private final S3IntegrityChecker integrityChecker = new S3IntegrityChecker();

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

  public final void finalizeUpload(String uploadId, List<PartETag> partETags) {
    CompleteMultipartUploadRequest completeRequest = finalizeUploadRequest(uploadId, partETags);
    CompleteMultipartUploadResult completeMultipartUploadResult = s3Client.completeMultipartUpload(completeRequest);
    if (config.isCheckIntegrity()) {
      integrityChecker.check(completeMultipartUploadResult.getETag(), partETags);
    }
  }

  public final void abortUpload(String uploadId) {
    AbortMultipartUploadRequest abortMultipartUploadRequest = abortUploadRequest(uploadId);
    s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }
}
