package org.gbif.pipelines.transforms;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.FieldValueExtractFn;

/** TODO:DOC */
public class EsTransforms {

  private EsTransforms() {}

  /** TODO:DOC */
  public static FieldValueExtractFn getEsTripletIdFn() {
    return input -> {
      String datasetKey = input.get("datasetKey").asText();
      String occurrenceId = input.get("occurrenceId").asText();

      String triplet = datasetKey + occurrenceId;

      MessageDigest messageDigest;
      try {
        messageDigest = MessageDigest.getInstance("SHA-1");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalArgumentException("Can't find SHA-1 algorithm");
      }
      return bytesToHex(messageDigest.digest(triplet.getBytes(StandardCharsets.UTF_8)));
    };
  }

  private static String bytesToHex(byte[] hash) {
    StringBuilder hexString = new StringBuilder();
    for (byte hash1 : hash) {
      String hex = Integer.toHexString(0xff & hash1);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }

}
