package org.gbif.pipelines.keygen;

import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

public interface HBaseLockingKey {

  KeyLookupResult generateKey(Set<String> uniqueStrings, String scope);

  KeyLookupResult generateKey(Set<String> uniqueStrings);

  KeyLookupResult findKey(Set<String> uniqueStrings, String scope);

  KeyLookupResult findKey(Set<String> uniqueStrings);

  Optional<KeyLookupResult> migrate(String oldLookupKey, String newLookupKey, String scope);

  Optional<KeyLookupResult> migrate(String oldLookupKey, String newLookupKey);

  void close();
}
