package me.bottaio.streamupload.s3;

import com.amazonaws.services.s3.model.PartETag;
import me.bottaio.streamupload.StreamPart;
import me.bottaio.streamupload.StreamPartUploader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class S3StreamPartUploader implements StreamPartUploader {
  private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());
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
    awsFacade.finalizeUpload(uploadId, partETags);
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
