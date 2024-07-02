package org.gbif.pipelines.diagnostics.strategy;

import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LookupKeyUtils {

  public static Optional<Long> getKey(HBaseLockingKeyService service, String lookupKey) {
    Optional<Long> tripletKey = Optional.empty();
    if (lookupKey != null && !lookupKey.isEmpty()) {
      tripletKey = service.findKey(Set.of(lookupKey)).map(KeyLookupResult::getKey);
    }
    return tripletKey;
  }
}
