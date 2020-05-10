package bottaio.streamupload.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PartETag;
import lombok.Builder;
import lombok.Data;
import bottaio.streamupload.ConvertibleOutputStream;
import bottaio.streamupload.StreamPart;
import bottaio.streamupload.StreamTransferManager.Config;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Collections;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
public class S3IntegrityCheckerSpec {

  @Container
  private static final LocalStackContainer localstack = new LocalStackContainer()
      .withServices(S3);

  @Test
  public void shouldRejectInvalidEtagWhenCheckingIntegrity() throws IOException {
    AwsFacade facade = buildFacade(true);
    String id = setup(facade).getId();

    AmazonS3Exception exception = assertThrows(
        AmazonS3Exception.class,
        () -> facade.finalizeUpload(id, Collections.singletonList(new PartETag(1, "invalid")))
    );

    assertTrue(exception.getMessage().contains("The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."));
  }

  // todo: this test always fails because S3 is validating etags of each part
  @Test
  @Disabled
  public void shouldAcceptInvalidEtagWhenNotCheckingIntegrity() throws IOException {
    AwsFacade facade = buildFacade(false);
    String id = setup(facade).getId();

    assertDoesNotThrow(
        () -> facade.finalizeUpload(id, Collections.singletonList(new PartETag(1, "invalid")))
    );
  }

  @Test
  public void shouldAcceptValidEtagWhenCheckingIntegrity() throws IOException {
    AwsFacade facade = buildFacade(true);
    SetupResult result = setup(facade);

    assertDoesNotThrow(
        () -> facade.finalizeUpload(result.getId(), Collections.singletonList(result.getEtag()))
    );
  }

  private AwsFacade buildFacade(boolean checkIntegrity) {
    String bucketName = "bucket";
    String key = "key";

    AmazonS3 client = AmazonS3ClientBuilder.standard()
        .withCredentials(localstack.getDefaultCredentialsProvider())
        .withEndpointConfiguration(localstack.getEndpointConfiguration(S3))
        .build();
    client.createBucket(bucketName);

    Config config = Config.builder()
        .bucketName(bucketName)
        .putKey(key)
        .partSize(10 * MB)
        .checkIntegrity(checkIntegrity)
        .build();

    return new AwsFacade(config, client);
  }

  private SetupResult setup(AwsFacade facade) throws IOException {
    String id = facade.initializeUpload();

    ConvertibleOutputStream stream = new ConvertibleOutputStream(10);
    stream.write("test".getBytes());
    PartETag etag = facade.uploadPart(id, new StreamPart(stream, 1));

    return SetupResult.builder()
        .id(id)
        .etag(etag)
        .build();
  }

  @Data
  @Builder
  private static class SetupResult {
    private final String id;
    private final PartETag etag;
  }
}
