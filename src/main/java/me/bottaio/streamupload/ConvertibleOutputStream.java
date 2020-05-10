package me.bottaio.streamupload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * A ByteArrayOutputStream with some useful additional functionality.
 */
class ConvertibleOutputStream extends ByteArrayOutputStream {

  private static final Logger log = LoggerFactory.getLogger(ConvertibleOutputStream.class);

  public ConvertibleOutputStream(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Creates an InputStream sharing the same underlying byte array, reducing memory usage and copying time.
   */
  public InputStream toInputStream() {
    return new ByteArrayInputStream(buf, 0, count);
  }

  /**
   * Truncates this stream to a given size and returns a new stream containing a copy of the remaining data.
   *
   * @param countToKeep                 number of bytes to keep in this stream, starting from the first written byte.
   * @param initialCapacityForNewStream buffer capacity to construct the new stream (NOT the number of bytes
   *                                    that the new stream will take from this one)
   * @return a new stream containing all the bytes previously contained in this one, i.e. from countToKeep + 1 onwards.
   */
  public ConvertibleOutputStream split(int countToKeep, int initialCapacityForNewStream) {
    int newCount = count - countToKeep;
    log.info("Splitting stream of size {} into parts with sizes {} and {}", count, countToKeep, newCount);
    initialCapacityForNewStream = Math.max(initialCapacityForNewStream, newCount);
    ConvertibleOutputStream newStream = new ConvertibleOutputStream(initialCapacityForNewStream);
    newStream.write(buf, countToKeep, newCount);
    count = countToKeep;
    return newStream;
  }

  public byte[] getMD5Digest() {
    MessageDigest md = Utils.md5();
    md.update(buf, 0, count);
    return md.digest();
  }

}