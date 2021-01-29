package org.gbif.pipelines.diagnostics.strategy;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public class BothStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    Set<String> keys = new HashSet<>(2);
    if (!onlyCollisions) {
      tripletKey.ifPresent(x -> keys.add(triplet));
      occurrenceIdtKey.ifPresent(x -> keys.add(occurrenceId));
    }

    if (onlyCollisions
        && tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.add(triplet);
      keys.add(occurrenceId);
    }

    return keys;
  }
}
