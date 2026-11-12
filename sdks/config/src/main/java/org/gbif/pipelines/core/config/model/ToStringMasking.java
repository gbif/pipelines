package org.gbif.pipelines.core.config.model;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class ToStringMasking {

  // Header names are case-insensitive, so compare in lower case
  private static final Set<String> SENSITIVE_HEADERS =
      Set.of("authorization", "proxy-authorization", "x-api-key", "apikey", "cookie");

  private ToStringMasking() {}

  static Map<String, String> maskHeaders(Map<String, String> headers) {
    if (headers == null) {
      return null;
    }
    Map<String, String> masked = new LinkedHashMap<>();
    headers.forEach(
        (k, v) ->
            masked.put(
                k,
                k != null && SENSITIVE_HEADERS.contains(k.toLowerCase(Locale.ROOT)) ? "****" : v));
    return masked;
  }
}
