package org.gbif.pipelines.diagnostics.strategy;

import java.util.Map;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public interface DeletionStrategy {

  Map<String, Long> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId);

  enum DeletionStrategyType {
    MIN(new MinStrategy()),
    MAX(new MaxStrategy()),
    OCCURRENCE_ID(new OccurrenceIdStrategy()),
    TRIPLET(new TripletStrategy()),
    BOTH(new BothStrategy());

    private final DeletionStrategy deletionStrategy;

    DeletionStrategyType(DeletionStrategy deletionStrategy) {
      this.deletionStrategy = deletionStrategy;
    }

    public Map<String, Long> getKeysToDelete(
        HBaseLockingKeyService keygenService,
        boolean onlyCollisions,
        String triplet,
        String occurrenceId) {
      return deletionStrategy.getKeysToDelete(keygenService, onlyCollisions, triplet, occurrenceId);
    }
  }
}
