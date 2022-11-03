package org.gbif.pipelines.keygen.identifier;

import com.google.common.base.Strings;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** A helper class for building the row keys used in the occurrence lookup table/process. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceKeyBuilder {

  private static final char DELIMITER = '|';

  public static String buildKeyPrefix(String datasetKey) {
    return datasetKey + DELIMITER;
  }

  public static Set<String> buildKeys(Set<String> uniqueStrings, String scope) {
    return uniqueStrings.stream().map(us -> scope + DELIMITER + us).collect(Collectors.toSet());
  }

  public static Optional<String> buildKey(
      String institutionCode, String collectionCode, String catalogNumber) {

    if (Strings.isNullOrEmpty(institutionCode)
        || Strings.isNullOrEmpty(collectionCode)
        || Strings.isNullOrEmpty(catalogNumber)) {
      return Optional.empty();
    }

    String key =
        institutionCode
            + DELIMITER
            + collectionCode
            + DELIMITER
            + catalogNumber
            + DELIMITER
            + null; // To support backward compatibility

    return Optional.of(key);
  }
}
