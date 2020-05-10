package com.github.bottaio.streamupload;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.IOUtils;
import com.github.bottaio.streamupload.s3.AwsFacade;
import com.github.bottaio.streamupload.s3.S3StreamPartUploader;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.inject.Module;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.Random;

import static com.amazonaws.services.s3.internal.Constants.MB;
import static com.github.bottaio.streamupload.StreamTransferManager.Config;

/**
 * A WIP test using s3proxy to avoid requiring actually connecting to a real S3 bucket.
 */
public class StreamTransferManagerTest {

  static {
    System.setProperty(
        SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,
        "true");
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private URI s3Endpoint;
  private S3Proxy s3Proxy;
  private BlobStoreContext context;
  private String containerName;
  private String key;
  private BasicAWSCredentials awsCreds;

  private static String createRandomContainerName() {
    return "s3proxy-" + new Random().nextInt(Integer.MAX_VALUE);
  }

  @Before
  public void setUp() throws Exception {
    Properties s3ProxyProperties = new Properties();
    InputStream is = Resources.asByteSource(Resources.getResource(
        "s3proxy.conf")).openStream();
    try {
      s3ProxyProperties.load(is);
    } finally {
      is.close();
    }

    String provider = s3ProxyProperties.getProperty(
        Constants.PROPERTY_PROVIDER);
    String identity = s3ProxyProperties.getProperty(
        Constants.PROPERTY_IDENTITY);
    String credential = s3ProxyProperties.getProperty(
        Constants.PROPERTY_CREDENTIAL);
    String endpoint = s3ProxyProperties.getProperty(
        Constants.PROPERTY_ENDPOINT);
    AuthenticationType s3Authorization = AuthenticationType.valueOf(
        CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_UNDERSCORE,
            s3ProxyProperties.getProperty(S3ProxyConstants.PROPERTY_AUTHORIZATION))
    );
    String s3Identity = s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_IDENTITY);
    String s3Credential = s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_CREDENTIAL);
    awsCreds = new BasicAWSCredentials(s3Identity, s3Credential);
    s3Endpoint = new URI(s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_ENDPOINT));
    String keyStorePath = s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
    String keyStorePassword = s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
    String virtualHost = s3ProxyProperties.getProperty(
        S3ProxyConstants.PROPERTY_VIRTUAL_HOST);

    ContextBuilder builder = ContextBuilder
        .newBuilder(provider)
        .credentials(identity, credential)
        .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
        .overrides(s3ProxyProperties);
    if (!Strings.isNullOrEmpty(endpoint)) {
      builder.endpoint(endpoint);
    }
    context = builder.build(BlobStoreContext.class);
    BlobStore blobStore = context.getBlobStore();
    containerName = createRandomContainerName();
    key = "stuff";
    blobStore.createContainerInLocation(null, containerName);

    S3Proxy.Builder s3ProxyBuilder = S3Proxy.builder()
        .blobStore(blobStore)
        .endpoint(s3Endpoint);
    //noinspection ConstantConditions
    if (s3Identity != null || s3Credential != null) {
      s3ProxyBuilder.awsAuthentication(s3Authorization, s3Identity, s3Credential);
    }
    if (keyStorePath != null || keyStorePassword != null) {
      s3ProxyBuilder.keyStore(
          Resources.getResource(keyStorePath).toString(),
          keyStorePassword);
    }
    if (virtualHost != null) {
      s3ProxyBuilder.virtualHost(virtualHost);
    }
    s3Proxy = s3ProxyBuilder.build();
    s3Proxy.start();
    while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
      Thread.sleep(1);
    }

    // reset endpoint to handle zero port
    s3Endpoint = new URI(s3Endpoint.getScheme(), s3Endpoint.getUserInfo(),
        s3Endpoint.getHost(), s3Proxy.getPort(), s3Endpoint.getPath(),
        s3Endpoint.getQuery(), s3Endpoint.getFragment());
  }

  @After
  public void tearDown() throws Exception {
    if (s3Proxy != null) {
      s3Proxy.stop();
    }
    if (context != null) {
      context.getBlobStore().deleteContainer(containerName);
      context.close();
    }
  }

  @Test
  public void testTransferManager() throws Exception {
    testTransferManager(1000000);
    testTransferManager(0);
  }

  private void testTransferManager(final int numLines) throws Exception {
    AmazonS3 client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withClientConfiguration(new ClientConfiguration().withSignerOverride("S3SignerType"))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Endpoint.toString(),
            AwsHostNameUtils.parseRegion(s3Endpoint.toString(), null)))
        .enablePathStyleAccess()
        .build();

    Config config = Config.builder()
        .bucketName(containerName)
        .putKey(key)
        .partSize(10 * MB)
        .checkIntegrity(true)
        .build();

    AwsFacade facade = new AwsFacade(config, client) {
      @Override
      protected UploadPartRequest uploadPartRequest(String uploadId, StreamPart part) {
        UploadPartRequest request = super.uploadPartRequest(uploadId, part);
        /*
        Workaround from https://github.com/andrewgaul/s3proxy/commit/50a302436271ec46ce81a415b4208b9e14fcaca4
        to deal with https://github.com/andrewgaul/s3proxy/issues/80
         */
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/unknown");
        request.setObjectMetadata(metadata);

        return request;
      }
    };

    final StreamTransferManager manager = new StreamTransferManager(config, new S3StreamPartUploader(facade));

    final MultipartOutputStream stream = manager.getMultiPartOutputStream();
    final StringBuilder builder = new StringBuilder();
    for (int lineNum = 0; lineNum < numLines; lineNum++) {
      String line = String.format("Stream, line %d\n", lineNum);
      stream.write(line.getBytes());
      builder.append(line);
    }
    stream.close();
    manager.complete();

    String expectedResult = builder.toString();

    S3ObjectInputStream objectContent = client.getObject(containerName, key).getObjectContent();
    String result = IOUtils.toString(objectContent);
    IOUtils.closeQuietly(objectContent, null);

    Assert.assertEquals(expectedResult, result);
  }

}
