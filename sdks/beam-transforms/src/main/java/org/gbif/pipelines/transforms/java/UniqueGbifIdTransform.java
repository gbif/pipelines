package org.gbif.pipelines.transforms.java;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_GBIF_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.INVALID_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

/**
 * Splits collection into two: 1 - normal collection with regular GBIF ids 2 - contains invalid
 * records with GBIF ids, as duplicates or missed GBIF ids
 */
@Slf4j
@Getter
@Builder
@SuppressWarnings("all")
public class UniqueGbifIdTransform {

  private final Map<String, IdentifierRecord> idMap = new ConcurrentHashMap<>();
  private final Map<String, IdentifierRecord> idInvalidMap = new ConcurrentHashMap<>();
  // keyed by the ExtendedRecord ID
  private final Map<String, IdentifierRecord> erIdMap = new ConcurrentHashMap<>();

  @NonNull private Function<ExtendedRecord, Optional<IdentifierRecord>> idTransformFn;

  @NonNull private Map<String, ExtendedRecord> erMap;

  @Builder.Default private ExecutorService executor = Executors.newWorkStealingPool();

  @Builder.Default private boolean useSyncMode = true;

  @Builder.Default private boolean skipTransform = false;

  private SerializableConsumer<String> counterFn;

  public UniqueGbifIdTransform run() {
    return useSyncMode ? runSync() : runAsync();
  }

  @SneakyThrows
  private UniqueGbifIdTransform runAsync() {
    // Filter GBIF id duplicates
    Consumer<ExtendedRecord> interpretIdFn = filterByGbifId();

    // Run async
    CompletableFuture<?>[] idFutures =
        erMap.values().stream()
            .map(v -> CompletableFuture.runAsync(() -> interpretIdFn.accept(v), executor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(idFutures).get();

    return this;
  }

  @SneakyThrows
  private UniqueGbifIdTransform runSync() {
    erMap.values().forEach(filterByGbifId());

    return this;
  }

  /** Process GBIF id duplicates */
  private Consumer<ExtendedRecord> filterByGbifId() {
    return er ->
        idTransformFn
            .apply(er)
            .ifPresent(
                id -> {
                  if (skipTransform) {
                    idMap.put(id.getId(), id);
                  } else if (id.getInternalId() != null) {
                    filter(id);
                  } else {
                    incMetrics(INVALID_GBIF_ID_COUNT);
                    idInvalidMap.put(id.getId(), id);
                    log.error("GBIF ID is null, occurrenceId - {}", id.getId());
                  }
                  erIdMap.put(er.getId(), id);
                });
  }

  /** Filter GBIF id duplicates if it is exist */
  private void filter(IdentifierRecord id) {
    IdentifierRecord record = idMap.get(id.getInternalId());
    if (record != null) {
      int compare =
          HashConverter.getSha1(id.getId()).compareTo(HashConverter.getSha1(record.getId()));
      if (compare < 0) {
        incMetrics(IDENTICAL_GBIF_OBJECTS_COUNT);
        idMap.put(id.getInternalId(), id);
        idInvalidMap.put(record.getId(), record);
      } else {
        incMetrics(DUPLICATE_GBIF_IDS_COUNT);
        idInvalidMap.put(id.getId(), id);
      }
      log.error(
          "GBIF ID collision, gbifId - {}, occurrenceId - {}", id.getInternalId(), id.getId());
    } else {
      incMetrics(UNIQUE_GBIF_IDS_COUNT);
      idMap.put(id.getInternalId(), id);
    }
  }

  private void incMetrics(String metricName) {
    Optional.ofNullable(counterFn).ifPresent(x -> x.accept(metricName));
  }
}
