package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
public class AllStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {

    Set<String> keys = new HashSet<>(2);

    if (triplet != null
        && !triplet.isEmpty()
        && keygenService.findKey(Collections.singleton(triplet)) != null) {
      keys.add(triplet);
    }

    if (occurrenceId != null
        && !occurrenceId.isEmpty()
        && keygenService.findKey(Collections.singleton(occurrenceId)) != null) {
      keys.add(occurrenceId);
    }

    return keys;
  }
}
