package org.gbif.pipelines.diagnostics.strategy;

import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
public class TripletStrategy implements DeletionStrategy {
  @Override
  public Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {

    if (triplet != null
        && !triplet.isEmpty()
        && keygenService.findKey(Collections.singleton(triplet)) != null) {
      return Collections.singleton(triplet);
    }
    return Collections.emptySet();
  }
}
