package me.bottaio.streamupload;

/**
 * Thrown when final integrity check fails. It suggests that the multipart upload failed
 * due to data corruption.
 */
public class IntegrityCheckException extends RuntimeException {

  public IntegrityCheckException(String message) {
    super(message);
  }
}
