package org.gbif.pipelines.spark.udf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.spark.sql.api.java.UDF1;

public class Base64DecodeUDF implements UDF1<String, String> {

  @Override
  public String call(String encoded) {
    if (encoded == null) {
      return null;
    }

    try {
      byte[] decodedBytes = Base64.getDecoder().decode(encoded);
      return new String(decodedBytes, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      // Invalid Base64 input
      return null;
    }
  }
}
