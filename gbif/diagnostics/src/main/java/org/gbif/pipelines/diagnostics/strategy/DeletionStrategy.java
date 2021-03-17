package org.gbif.pipelines.diagnostics.strategy;

import java.util.Set;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public interface DeletionStrategy {

  Set<String> getKeysToDelete(
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

    public Set<String> getKeysToDelete(
        HBaseLockingKeyService keygenService,
        boolean onlyCollisions,
        String triplet,
        String occurrenceId) {
      return deletionStrategy.getKeysToDelete(keygenService, onlyCollisions, triplet, occurrenceId);
    }
  }
}
