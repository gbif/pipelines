package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.DynamicProperty;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/total_length_parser.py
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LengthParser {

  public static Optional<DynamicProperty> parse(String source) {
    try {
      return Optional.empty();
    } catch (RuntimeException ex) {
      return Optional.empty();
    }
  }
}
