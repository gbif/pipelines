package org.gbif.pipelines.diagnostics.strategy;

import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
public class TripletStrategy implements DeletionStrategy {
  @Override
  public void deleteKeys(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {
    log.info("TripletStrategy");
  }
}
