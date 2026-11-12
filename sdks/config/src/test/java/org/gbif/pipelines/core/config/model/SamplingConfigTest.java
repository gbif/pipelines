package org.gbif.pipelines.core.config.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SamplingConfigTest {
  @Test
  void testSensitiveHeadersMaskedToString() {
    SamplingConfig samplingConfig = new SamplingConfig();

    String token = "Bearer SuperSecretToken";
    String apiKey = "SuperSecretApiKey";
    String userAgent = "gbif-pipelines";

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", token);
    headers.put("X-Api-Key", apiKey);
    headers.put("User-Agent", userAgent);
    samplingConfig.setHttpHeaders(headers);

    String wsUrl = "https://sampling.gbif.org/";
    samplingConfig.setWsUrl(wsUrl);

    String toString = samplingConfig.toString();

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
  void testOriginalHeadersNotModified() {
    SamplingConfig samplingConfig = new SamplingConfig();

    String token = "Bearer SuperSecretToken";
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", token);
    samplingConfig.setHttpHeaders(headers);

    samplingConfig.toString();

    // Masking must only affect toString, not the value actually sent in requests
    assertEquals(token, samplingConfig.getHttpHeaders().get("Authorization"));
  }

  @Test
  void testNullHeadersToString() {
    SamplingConfig samplingConfig = new SamplingConfig();
    samplingConfig.setHttpHeaders(null);

    String toString = assertDoesNotThrow(samplingConfig::toString);
    assertTrue(toString.contains("httpHeaders=null"));
  }
}
