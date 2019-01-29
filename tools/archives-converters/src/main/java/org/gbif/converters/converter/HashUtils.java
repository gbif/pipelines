package org.gbif.converters.converter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {

  private HashUtils() {}

  public static String getSha1(String... strings) {
    return getHash("SHA-1", strings);
  }

  private static String getHash(String algorithm, String... strings) {
    String join = String.join("", strings);
    try {
      MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
      byte[] digest = messageDigest.digest(join.getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (byte hash : digest) {
        String hex = Integer.toHexString(0xff & hash);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Can't find " + algorithm + " algorithm");
    }
  }
}
