package org.gbif.pipelines.diagnostics.strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class OccurrenceIdStrategy implements DeletionStrategy {
  @Override
  public Map<String, Long> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    Map<String, Long> keys = new HashMap<>(1);
    if (!onlyCollisions) {
      occurrenceIdtKey.ifPresent(x -> keys.put(occurrenceId, x));
      return keys;
    }

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);

    if (tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.put(occurrenceId, occurrenceIdtKey.get());
    }

    return keys;
  }
}
