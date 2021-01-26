package org.gbif.pipelines.diagnostics.strategy;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class TripletStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);

    Set<String> keys = new HashSet<>(1);
    if (!onlyCollisions) {
      tripletKey.ifPresent(x -> keys.add(triplet));
      return keys;
    }

    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    if (tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.add(triplet);
    }

    return keys;
  }
}
