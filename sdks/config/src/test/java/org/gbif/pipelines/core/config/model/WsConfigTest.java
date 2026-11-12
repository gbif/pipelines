package org.gbif.pipelines.core.config.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WsConfigTest {
  @Test
  void testSensitiveHeadersMaskedToString() {
    WsConfig wsConfig = new WsConfig();

    String token = "Bearer SuperSecretToken";
    String apiKey = "SuperSecretApiKey";
    String userAgent = "gbif-pipelines";

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", token);
    headers.put("X-Api-Key", apiKey);
    headers.put("User-Agent", userAgent);
    wsConfig.setHttpHeaders(headers);

    String wsUrl = "https://api.gbif.org/v1/";
    wsConfig.setWsUrl(wsUrl);

    String toString = wsConfig.toString();

    // Not to contain (Sensitive)
    assertFalse(toString.contains(token));
    assertFalse(toString.contains(apiKey));

    // Sensitive keys still visible, but masked
    assertTrue(toString.contains("Authorization=****"));
    assertTrue(toString.contains("X-Api-Key=****"));

    // Fine to contain
    assertTrue(toString.contains("User-Agent=" + userAgent));
    assertTrue(toString.contains("httpHeaders="));
    assertTrue(toString.contains(wsUrl));
  }

  @Test
  void testSensitiveHeadersMaskedCaseInsensitive() {
    WsConfig wsConfig = new WsConfig();

    String token = "Bearer SuperSecretToken";
    wsConfig.setHttpHeaders(Map.of("AUTHORIZATION", token));

    String toString = wsConfig.toString();

    assertFalse(toString.contains(token));
    assertTrue(toString.contains("AUTHORIZATION=****"));
  }

  @Test
  void testOriginalHeadersNotModified() {
    WsConfig wsConfig = new WsConfig();

    String token = "Bearer SuperSecretToken";
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", token);
    wsConfig.setHttpHeaders(headers);

    wsConfig.toString();

    // Masking must only affect toString, not the value actually sent in requests
    assertEquals(token, wsConfig.getHttpHeaders().get("Authorization"));
  }

  @Test
  void testNullHeadersToString() {
    WsConfig wsConfig = new WsConfig();
    wsConfig.setHttpHeaders(null);

    String toString = assertDoesNotThrow(wsConfig::toString);
    assertTrue(toString.contains("httpHeaders=null"));
  }
}
