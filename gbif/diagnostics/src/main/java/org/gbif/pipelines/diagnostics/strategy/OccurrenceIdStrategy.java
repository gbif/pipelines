package org.gbif.pipelines.diagnostics.strategy;

import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class OccurrenceIdStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId) {

    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occurrenceId);

    Set<String> keys = new HashSet<>(1);
    if (!onlyCollisions) {
      occurrenceIdtKey.ifPresent(x -> keys.add(occurrenceId));
      return keys;
    }

    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);

    if (tripletKey.isPresent()
        && occurrenceIdtKey.isPresent()
        && !occurrenceIdtKey.get().equals(tripletKey.get())) {
      keys.add(occurrenceId);
    }

    return keys;
  }
}
