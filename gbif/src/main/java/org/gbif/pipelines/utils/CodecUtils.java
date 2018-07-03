package org.gbif.pipelines.utils;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;

public class CodecUtils {

  private CodecUtils() {
  }

  // avro codecs
  private static final String DEFLATE = "deflate";
  private static final String SNAPPY = "snappy";
  private static final String BZIP2 = "bzip2";
  private static final String XZ = "xz";
  private static final String NULL = "null";
  private static final String CODEC_SEPARATOR = "_";

  public static CodecFactory parseAvroCodec(String codec) {
    if (Strings.isNullOrEmpty(codec) || NULL.equalsIgnoreCase(codec)) {
      return CodecFactory.nullCodec();
    }

    if (SNAPPY.equalsIgnoreCase(codec)) {
      return CodecFactory.snappyCodec();
    }

    if (BZIP2.equalsIgnoreCase(codec)) {
      return CodecFactory.bzip2Codec();
    }

    if (codec.toLowerCase().startsWith(DEFLATE)) {
      List<String> pieces = Splitter.on(CODEC_SEPARATOR).splitToList(codec);
      int compressionLevel = CodecFactory.DEFAULT_DEFLATE_LEVEL;
      if (pieces.size() > 1) {
        compressionLevel = Integer.parseInt(pieces.get(1));
      }
      return CodecFactory.deflateCodec(compressionLevel);
    }

    if (codec.toLowerCase().startsWith(XZ)) {
      List<String> pieces = Splitter.on(CODEC_SEPARATOR).splitToList(codec);
      int compressionLevel = CodecFactory.DEFAULT_XZ_LEVEL;
      if (pieces.size() > 1) {
        compressionLevel = Integer.parseInt(pieces.get(1));
      }
      return CodecFactory.xzCodec(compressionLevel);
    }

    throw new IllegalArgumentException("CodecFactory not found for codec " + codec);
  }

}
