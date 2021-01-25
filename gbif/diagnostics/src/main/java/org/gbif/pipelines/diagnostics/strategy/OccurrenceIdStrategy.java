package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
public class OccurrenceIdStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {

    if (occurrenceId != null
        && !occurrenceId.isEmpty()
        && keygenService.findKey(Collections.singleton(occurrenceId)) != null) {
      return Collections.singleton(occurrenceId);
    }

    return Collections.emptySet();
  }
}
