package bottaio.s3upload;

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

  public static final int S3_MIN_PART_SIZE = 5 * MB;
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
    if (partSize < S3_MIN_PART_SIZE) {
      throw new IllegalArgumentException(String.format(
          "The given part size (%d) is less than 5 MB.", partSize));
    }

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
    This consists of the size that the user asks for, the extra 5 MB to avoid small parts (see the comment in
    checkSize()), and some extra space to make resizing and copying unlikely.
     */
    return partSize + S3_MIN_PART_SIZE + STREAM_EXTRA_ROOM;
  }

  /**
   * Checks if the stream currently contains enough data to create a new part.
   */
  private void checkSize() {
    /*
    This class avoids producing parts < 5 MB if possible by only producing a part when it has an extra 5 MB to spare
    for the next part. For example, suppose the following. A stream is producing parts of 10 MB. Someone writes
    10 MB and then calls this method, and then writes only 3 MB more before closing. If the initial 10 MB were
    immediately packaged into a StreamPart and a new ConvertibleOutputStream was started on for the rest, it would
    end up producing a part with just 3 MB. So instead the class waits until it contains 15 MB of data and then it
    splits the stream into two: one with 10 MB that gets produced as a part, and one with 5 MB that it continues with.
    In this way users of the class are less likely to encounter parts < 5 MB which cause trouble: see the caveat
    on order in the StreamTransferManager and the way it handles these small parts, referred to as 'leftover'.
    Such parts are only produced when the user closes a stream that never had more than 5 MB written to it.
     */
    if (currentStream == null) {
      throw new IllegalStateException("The stream is closed and cannot be written to.");
    }
    if (currentStream.size() > partSize + S3_MIN_PART_SIZE) {
      ConvertibleOutputStream newStream = currentStream.split(
          currentStream.size() - S3_MIN_PART_SIZE,
          getStreamAllocatedSize());
      putCurrentStream();
      currentStream = newStream;
    }
  }

  private void putCurrentStream() {
    if (currentStream.size() == 0) {
      return;
    }
    StreamPart streamPart = new StreamPart(currentStream, currentPartNumber++);
    log.debug("Putting {} on queue", streamPart);
    uploader.upload(streamPart);
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

    putCurrentStream();
    currentStream = null;
  }

  @Override
  public String toString() {
    return "[MultipartOutputStream]";
  }
}
