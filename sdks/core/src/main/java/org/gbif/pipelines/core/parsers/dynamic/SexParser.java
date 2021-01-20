package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;

public class SexParser {

  public static Optional<String> parseSex(String value) {
    if (value == null || value.isEmpty()) {
      return Optional.empty();
    }
    return Optional.empty();
  }
}
