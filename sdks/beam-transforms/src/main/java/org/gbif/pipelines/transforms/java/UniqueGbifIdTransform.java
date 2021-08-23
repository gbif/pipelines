package org.gbif.pipelines.transforms.java;

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
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Splits collection into two: 1 - normal collection with regular GBIF ids 2 - contains invalid
 * records with GBIF ids, as duplicates or missed GBIF ids
 */
@Slf4j
@Getter
@Builder
@SuppressWarnings("all")
public class UniqueGbifIdTransform {

  private final Map<String, BasicRecord> brMap = new ConcurrentHashMap<>();
  private final Map<String, BasicRecord> brInvalidMap = new ConcurrentHashMap<>();
  // keyed by the ExtendedRecord ID
  private final Map<String, BasicRecord> erBrMap = new ConcurrentHashMap<>();

  @NonNull private Function<ExtendedRecord, Optional<BasicRecord>> basicTransformFn;

  @NonNull private Map<String, ExtendedRecord> erMap;

  @Builder.Default private ExecutorService executor = Executors.newWorkStealingPool();

  @Builder.Default private boolean useSyncMode = true;

  @Builder.Default private boolean skipTransform = false;

  public UniqueGbifIdTransform run() {
    return useSyncMode ? runSync() : runAsync();
  }

  @SneakyThrows
  private UniqueGbifIdTransform runAsync() {
    // Filter GBIF id duplicates
    Consumer<ExtendedRecord> interpretBrFn = filterByGbifId();

    // Run async
    CompletableFuture<?>[] brFutures =
        erMap.values().stream()
            .map(v -> CompletableFuture.runAsync(() -> interpretBrFn.accept(v), executor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(brFutures).get();

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
        basicTransformFn
            .apply(er)
            .ifPresent(
                br -> {
                  if (skipTransform) {
                    brMap.put(br.getId(), br);
                  } else if (br.getGbifId() != null) {
                    filter(br);
                  } else {
                    brInvalidMap.put(br.getId(), br);
                    log.error("GBIF ID is null, occurrenceId - {}", br.getId());
                  }
                  erBrMap.put(er.getId(), br);
                });
  }

  /** Filter GBIF id duplicates if it is exist */
  private void filter(BasicRecord br) {
    BasicRecord record = brMap.get(br.getGbifId().toString());
    if (record != null) {
      int compare =
          HashConverter.getSha1(br.getId()).compareTo(HashConverter.getSha1(record.getId()));
      if (compare < 0) {
        brMap.put(br.getGbifId().toString(), br);
        brInvalidMap.put(record.getId(), record);
      } else {
        brInvalidMap.put(br.getId(), br);
      }
      log.error("GBIF ID collision, gbifId - {}, occurrenceId - {}", br.getGbifId(), br.getId());
    } else {
      brMap.put(br.getGbifId().toString(), br);
    }
  }
}
