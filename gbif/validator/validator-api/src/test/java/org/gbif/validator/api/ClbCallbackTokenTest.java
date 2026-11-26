package org.gbif.validator.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class ClbCallbackTokenTest {

  private static final UUID KEY = UUID.randomUUID();

  @Test
  void validToken() {
    String token = ClbCallbackToken.create(KEY, "secret");
    assertTrue(ClbCallbackToken.isValid(KEY, token, "secret"));
  }

  @Test
  void invalidToken() {
    String token = ClbCallbackToken.create(KEY, "secret");
    assertFalse(ClbCallbackToken.isValid(KEY, token, "other"));
    assertFalse(ClbCallbackToken.isValid(UUID.randomUUID(), token, "secret"));
    assertFalse(ClbCallbackToken.isValid(KEY, null, "secret"));
  }

  @Test
  void callbackUrl() {
    assertEquals("http://ws/cb/" + KEY, ClbCallbackToken.callbackUrl("http://ws/cb", KEY, null));
    assertEquals(
        "http://ws/cb/" + KEY + "/" + ClbCallbackToken.create(KEY, "secret"),
        ClbCallbackToken.callbackUrl("http://ws/cb", KEY, "secret"));
  }
}
