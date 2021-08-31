package org.gbif.pipelines.diagnostics.strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class TripletStrategy implements DeletionStrategy {
  @Override
  public Map<String, Long> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);

    Map<String, Long> keys = new HashMap<>(1);
    if (!onlyCollisions) {
      tripletKey.ifPresent(x -> keys.put(triplet, x));
      return keys;
    }

    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    if (tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.put(triplet, tripletKey.get());
    }

    return keys;
  }
}
