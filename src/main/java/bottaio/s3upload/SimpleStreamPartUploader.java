package bottaio.s3upload;

import com.amazonaws.services.s3.model.PartETag;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class SimpleStreamPartUploader implements StreamPartUploader {
  private final List<PartETag> partETags = new ArrayList<>();
  private final AwsFacade awsFacade;

  @Getter
  private String uploadId;

  public void initialize() {
    uploadId = awsFacade.initializeUpload();
  }

  @Override
  public void upload(StreamPart part) {
    log.debug("{}: Uploading {}", this, part);
    PartETag partETag = awsFacade.uploadPart(uploadId, part);
    partETags.add(partETag);
    log.info("{}: Finished uploading {}", this, part);
  }

  @Override
  public void complete() {
    log.debug("Completing");
    if (partETags.isEmpty()) {
      log.debug("Uploading empty stream");
      awsFacade.abortUpload(uploadId);
      awsFacade.putEmptyObject();
    } else {
      awsFacade.finalizeUpload(uploadId, partETags);
    }
    log.info("Completed");
  }

  @Override
  public void abort() {
    if (uploadId != null) {
      log.debug("Aborting");
      awsFacade.abortUpload(uploadId);
      log.info("Aborted");
    }
  }
}
