package bottaio.streamupload;

public interface StreamPartUploader {
  void initialize();

  void upload(StreamPart part);

  void complete();

  void abort();
}
