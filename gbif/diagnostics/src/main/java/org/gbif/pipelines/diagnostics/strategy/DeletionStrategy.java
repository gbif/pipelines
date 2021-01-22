package org.gbif.pipelines.diagnostics.strategy;

import org.gbif.pipelines.keygen.HBaseLockingKeyService;

public interface DeletionStrategy {

  void deleteKeys(HBaseLockingKeyService keygenService, String triplet, String occurrenceId);

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

    public void deleteKeys(
        HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {
      deletionStrategy.deleteKeys(keygenService, triplet, occurrenceId);
    }
  }
}
