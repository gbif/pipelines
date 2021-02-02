package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.DynamicProperty;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LengthParser {

  public static Optional<DynamicProperty> parse(String source) {
    return Optional.empty();
  }
}
