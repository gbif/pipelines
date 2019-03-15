package org.gbif.pipeleins.identifier;

import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A helper class for building the row keys used in the occurrence lookup table/process.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceKeyBuilder {

  private static final char DELIMITER = '|';

  public static String buildKeyPrefix(String datasetKey) {
    return datasetKey + DELIMITER;
  }

  public static Set<String> buildKeys(Set<String> uniqueStrings, String scope) {
    return uniqueStrings.stream()
        .map(us -> scope + DELIMITER + us)
        .collect(Collectors.toSet());
  }
}
