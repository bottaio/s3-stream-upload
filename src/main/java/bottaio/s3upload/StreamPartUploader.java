package bottaio.s3upload;

public interface StreamPartUploader {
  void initialize();

  String getUploadId();

  void upload(StreamPart part);

  void complete();

  void abort();
}
