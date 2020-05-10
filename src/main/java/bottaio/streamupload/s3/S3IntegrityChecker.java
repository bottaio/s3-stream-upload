package bottaio.streamupload.s3;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.util.BinaryUtils;
import bottaio.streamupload.IntegrityCheckException;
import bottaio.streamupload.Utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class S3IntegrityChecker {
  public void check(String s3ObjectETag, List<PartETag> partETags) {
    List<PartETag> parts = new ArrayList<>(partETags);
    parts.sort(Comparator.comparing(PartETag::getPartNumber));
    String expectedETag = computeCompleteFileETag(parts);
    if (!expectedETag.equals(s3ObjectETag)) {
      throw new IntegrityCheckException(String.format(
          "File upload completed, but integrity check failed. Expected ETag: %s but actual is %s",
          expectedETag, s3ObjectETag));
    }
  }

  private String computeCompleteFileETag(List<PartETag> parts) {
    // When S3 combines the parts of a multipart upload into the final object, the ETag value is set to the
    // hex-encoded MD5 hash of the concatenated binary-encoded (raw bytes) MD5 hashes of each part followed by
    // "-" and the number of parts.
    MessageDigest md = Utils.md5();
    for (PartETag partETag : parts) {
      md.update(BinaryUtils.fromHex(partETag.getETag()));
    }
    // Represent byte array as a 32-digit number hexadecimal format followed by "-<partCount>".
    return String.format("%032x-%d", new BigInteger(1, md.digest()), parts.size());
  }
}
