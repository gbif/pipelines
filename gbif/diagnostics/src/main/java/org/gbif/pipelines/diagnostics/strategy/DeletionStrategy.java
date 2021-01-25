package org.gbif.pipelines.diagnostics.strategy;

import org.gbif.pipelines.keygen.HBaseLockingKeyService;

import java.util.Set;

public interface DeletionStrategy {

  Set<String> getKeysToDelete(
      HBaseLockingKeyService keygenService,
      boolean onlyCollisions,
      String triplet,
      String occurrenceId);

  enum DeletionStrategyType {
    min(new MinStrategy()),
    max(new MaxStrategy()),
    occurrenceID(new OccurrenceIdStrategy()),
    triplet(new TripletStrategy()),
    all(new AllStrategy());

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
