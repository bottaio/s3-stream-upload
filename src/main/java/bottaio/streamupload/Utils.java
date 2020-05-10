package bottaio.streamupload;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Miscellaneous useful functions.
 */
public class Utils {
  public static MessageDigest md5() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.reset();
      return md;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
