package org.gbif.pipelines.diagnostics.strategy;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

import java.util.Collections;
import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LookupKeyUtils {

  public static Optional<Long> getKey(HBaseLockingKeyService service, String lookupKey) {
    Optional<Long> tripletKey = Optional.empty();
    if (lookupKey != null && !lookupKey.isEmpty()) {
      tripletKey =
          Optional.ofNullable(service.findKey(Collections.singleton(lookupKey)))
              .map(KeyLookupResult::getKey);
    }
    return tripletKey;
  }
}
