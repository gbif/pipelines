package org.gbif.validator.api;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Token that authenticates ChecklistBank callbacks: an HMAC of the validation key. */
public final class ClbCallbackToken {

  private static final String ALGORITHM = "HmacSHA256";

  private ClbCallbackToken() {}

  /** Creates the token of a validation key. */
  public static String create(UUID validationKey, String secret) {
    if (secret == null || secret.isEmpty()) {
      throw new IllegalArgumentException("Callback secret is not set");
    }
    try {
      Mac mac = Mac.getInstance(ALGORITHM);
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
      byte[] hmac = mac.doFinal(validationKey.toString().getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hmac);
    } catch (GeneralSecurityException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** True if the token is the token of the validation key. */
  public static boolean isValid(UUID validationKey, String token, String secret) {
    if (token == null || validationKey == null) {
      return false;
    }
    return MessageDigest.isEqual(
        create(validationKey, secret).getBytes(StandardCharsets.UTF_8),
        token.getBytes(StandardCharsets.UTF_8));
  }

  /** Callback URL of a validation, signed if a secret is set. */
  public static String callbackUrl(String baseUrl, UUID validationKey, String secret) {
    String url = baseUrl + "/" + validationKey;
    return secret == null || secret.isEmpty() ? url : url + "/" + create(validationKey, secret);
  }
}
