package org.gbif.pipeleins.keygen;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of KeyBuilder to provide keys for use as secondary indexes to occurrences in HBase.
 */
public class OccurrenceKeyBuilder {

  private static final String DELIM = "|";

  public Set<String> buildKeys(Set<String> uniqueStrings, String scope) {
    return uniqueStrings.stream()
        .map(us -> buildKey(us, scope))
        .collect(Collectors.toSet());
  }

  public String buildKey(String uniqueString, String scope) {
    return scope + DELIM + uniqueString;
  }
}
