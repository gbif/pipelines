package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MassParser {

  public static Optional<String> parse(String source) {
    return Optional.empty();
  }
}
