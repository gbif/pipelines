package org.gbif.pipelines.diagnostics.strategy;

import java.util.List;

public interface DeletionStrategy {

  List<Long> getKeysToDelete(String triplet, String occurrenceId);

  enum DeletionStrategyType {
    min(new MinStrategy()),
    max(new MaxStrategy()),
    occurrenceID(new OccurrenceIdStrategy()),
    triplet(new TripletStrategy()),
    all(new AllStrategy());

    private DeletionStrategy deletionStrategy;

    DeletionStrategyType(DeletionStrategy deletionStrategy) {
      this.deletionStrategy = deletionStrategy;
    }

    public List<Long> getKeysToDelete(String triplet, String occurrenceId) {
      return deletionStrategy.getKeysToDelete(triplet, occurrenceId);
    }
  }
}
