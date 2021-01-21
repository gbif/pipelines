package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;

public class LifeStageParser {

  public static Optional<String> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of("Adult");
  }
}
