package bottaio.s3upload;

import com.amazonaws.util.Base64;
import lombok.AllArgsConstructor;

import java.io.InputStream;

/**
 * A simple class which holds some data which can be uploaded to S3 as part of a multipart upload and a part number
 * identifying it.
 */
@AllArgsConstructor
public class StreamPart {

    private final ConvertibleOutputStream stream;
    private final int partNumber;

    public int getPartNumber() {
        return partNumber;
    }

    public InputStream getInputStream() {
        return stream.toInputStream();
    }

    public long size() {
        return stream.size();
    }

    public String getMD5Digest() {
        return Base64.encodeAsString(stream.getMD5Digest());
    }

    @Override
    public String toString() {
        return String.format("[Part number %d %s]", partNumber,
                stream == null ?
                        "with null stream" :
                        String.format("containing %.2f MB", size() / (1024 * 1024.0)));
    }
}
