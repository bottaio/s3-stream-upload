package com.github.bottaio.streamupload;

import lombok.extern.slf4j.Slf4j;

import java.io.OutputStream;

import static com.amazonaws.services.s3.internal.Constants.MB;

/**
 * An {@code OutputStream} which packages data written to it into discrete {@link StreamPart}s which can be obtained
 * in a separate thread via iteration and uploaded to S3.
 * <p>
 * A single {@code MultiPartOutputStream} is allocated a range of part numbers it can assign to the {@code StreamPart}s
 * it produces, which is determined at construction.
 * <p>
 * It's essential to call
 * {@link MultipartOutputStream#close()} when finished so that it can create the final {@code StreamPart} and consumers
 * can finish.
 * <p>
 * Writing to the stream may lead to trying to place a completed part on a queue,
 * which will block if the queue is full and may lead to an {@code InterruptedException}.
 */
@Slf4j
public class MultipartOutputStream extends OutputStream {
  private static final int STREAM_EXTRA_ROOM = MB;
  private final int partSize;
  private final StreamPartUploader uploader;
  private ConvertibleOutputStream currentStream;
  private int currentPartNumber;

  /**
   * Creates a new stream that will produce parts of the given size with part numbers in the given range.
   *
   * @param partSize the minimum size in bytes of parts to be produced.
   */
  MultipartOutputStream(int partSize, StreamPartUploader uploader) {
    this.partSize = partSize;
    this.uploader = uploader;
    this.currentPartNumber = 1;

    log.debug("Creating {}", this);
    currentStream = new ConvertibleOutputStream(getStreamAllocatedSize());
  }

  /**
   * Returns the initial capacity in bytes of the {@code ByteArrayOutputStream} that a part uses.
   */
  private int getStreamAllocatedSize() {
    /*
    This consists of the size that the user asks for and some extra space to make resizing and copying unlikely.
     */
    return partSize + STREAM_EXTRA_ROOM;
  }

  /**
   * Checks if the stream currently contains enough data to create a new part.
   */
  private void checkSize() {
    if (currentStream == null) {
      throw new IllegalStateException("The stream is closed and cannot be written to.");
    }
    if (currentStream.size() > partSize) {
      ConvertibleOutputStream newStream = currentStream.split(partSize, getStreamAllocatedSize());
      uploadCurrentStream(newStream);
    }
  }

  private void uploadCurrentStream(ConvertibleOutputStream newStream) {
    StreamPart streamPart = new StreamPart(currentStream, currentPartNumber++);
    log.debug("Putting {} on queue", streamPart);
    uploader.upload(streamPart);
    currentStream = newStream;
  }

  @Override
  public void write(int b) {
    currentStream.write(b);
    checkSize();
  }

  @Override
  public void write(byte[] b, int off, int len) {
    currentStream.write(b, off, len);
    checkSize();
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
    checkSize();
  }

  /**
   * Packages any remaining data into a {@link StreamPart} and signals to the {@code StreamTransferManager} that there are no more parts
   * afterwards. You cannot write to the stream after it has been closed.
   */
  @Override
  public void close() {
    log.info("Called close() on {}", this);
    if (currentStream == null) {
      log.warn("{} is already closed", this);
      return;
    }

    uploadCurrentStream(null);
  }

  @Override
  public String toString() {
    return "[MultipartOutputStream]";
  }
}
