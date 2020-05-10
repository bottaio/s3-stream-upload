package me.bottaio.streamupload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import me.bottaio.streamupload.s3.AwsFacade;
import me.bottaio.streamupload.s3.S3StreamPartUploader;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static me.bottaio.streamupload.StreamTransferManager.Config;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
public class StreamTransferManagerSpec {

  @Container
  private static final LocalStackContainer localstack = new LocalStackContainer()
      .withServices(S3);

  @ParameterizedTest
  @ValueSource(ints = {0, 100, 1000000, 3000000})
  public void testTransferManager(final int numLines) throws Exception {
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
        .checkIntegrity(true)
        .build();

    AwsFacade facade = new AwsFacade(config, client);
    StreamTransferManager manager = new StreamTransferManager(config, new S3StreamPartUploader(facade));

    String expectedResult;
    try (MultipartOutputStream stream = manager.getMultiPartOutputStream()) {
      StringBuilder builder = new StringBuilder();
      for (int lineNum = 0; lineNum < numLines; lineNum++) {
        String line = String.format("Stream, line %d\n", lineNum);
        stream.write(line.getBytes());
        builder.append(line);
      }
      expectedResult = builder.toString();
    }
    manager.complete();

    S3ObjectInputStream objectContent = client.getObject(bucketName, key).getObjectContent();
    String result = IOUtils.toString(objectContent);
    IOUtils.closeQuietly(objectContent, null);

    Assert.assertEquals(expectedResult, result);
  }
}
