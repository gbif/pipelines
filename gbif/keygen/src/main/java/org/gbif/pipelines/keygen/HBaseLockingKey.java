package org.gbif.pipelines.keygen;

import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

public interface HBaseLockingKey {

  KeyLookupResult generateKey(Set<String> uniqueStrings, String scope);

  KeyLookupResult generateKey(Set<String> uniqueStrings);

  Optional<KeyLookupResult> findKey(Set<String> uniqueStrings, String scope);

  Optional<KeyLookupResult> findKey(Set<String> uniqueStrings);

  void close();
}
