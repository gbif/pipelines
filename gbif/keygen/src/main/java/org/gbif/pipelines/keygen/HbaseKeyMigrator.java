package org.gbif.pipelines.keygen;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@Builder
public class HbaseKeyMigrator {

  private String fromDatasetKey;
  private String toDatasetKey;

  private String oldLookupKey;
  private String newLookupKey;

  private HBaseLockingKeyService keyService;

  @Builder.Default private boolean deleteKeys = false;

  public Optional<KeyLookupResult> migrate() {

    Set<String> oldLkSet = Collections.singleton(oldLookupKey);
    Set<String> newLkSet = Collections.singleton(newLookupKey);

    Optional<KeyLookupResult> oldKeyFromDataset = keyService.findKey(oldLkSet, fromDatasetKey);

    if (oldKeyFromDataset.isPresent()) {

      Optional<KeyLookupResult> newKeyToDataset = keyService.findKey(newLkSet, toDatasetKey);

      if (newKeyToDataset.isPresent() && !deleteKeys) {
        log.error(
            "The new lookup key already has a GBIF identifier, you must use the delete keys option otherwise there will be an id collision. Dataset key source {}, dataset key target {}, old lookup key {}, new lookup key {}",
            fromDatasetKey,
            toDatasetKey,
            oldLookupKey,
            newLookupKey);
        return Optional.empty();
      }

      // Deletion existing key is required
      keyService.deleteKeyByUniques(oldLkSet, fromDatasetKey);

      // Delete NEW possible keys
      if (deleteKeys) {
        newKeyToDataset.ifPresent(x -> keyService.deleteKeyByUniques(newLkSet, toDatasetKey));
      }

      OccurrenceKeyBuilder.buildKeys(newLkSet, toDatasetKey)
          .forEach(lookupKey -> keyService.putKey(lookupKey, oldKeyFromDataset.get().getKey()));

      log.info(
          "Migrate GBIF_ID {} from {} to {}",
          oldKeyFromDataset.get().getKey(),
          oldLookupKey,
          newLookupKey);

      return oldKeyFromDataset;
    } else {
      log.error("Can't find GBIF ID for datasetKey {}, lookup key {}", toDatasetKey, oldLookupKey);
      return Optional.empty();
    }
  }
}
