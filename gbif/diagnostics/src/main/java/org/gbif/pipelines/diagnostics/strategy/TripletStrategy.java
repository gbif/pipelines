package org.gbif.pipelines.diagnostics.strategy;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class TripletStrategy implements DeletionStrategy {
  @Override
  public List<Long> getKeysToDelete(String triplet, String occurrenceId) {
    log.info("TripletStrategy");
    return null;
  }
}
