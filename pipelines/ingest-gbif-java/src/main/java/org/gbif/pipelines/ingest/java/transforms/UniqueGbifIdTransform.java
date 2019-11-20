package org.gbif.pipelines.ingest.java.transforms;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.gbif.pipelines.core.utils.HashUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

@Getter
@Builder
public class UniqueGbifIdTransform {

  private final Map<Long, BasicRecord> brMap = new ConcurrentHashMap<>();
  private final Map<String, BasicRecord> brInvalidMap = new ConcurrentHashMap<>();

  @NonNull
  private BasicTransform basicTransform;

  @NonNull
  private HashMap<String, ExtendedRecord> erMap;

  @Builder.Default
  private ExecutorService executor = Executors.newWorkStealingPool();

  @SneakyThrows
  public UniqueGbifIdTransform run() {
    // Filter GBIF id duplicates
    Consumer<ExtendedRecord> interpretBrFn = filterByGbifId(brMap, brInvalidMap);

    // Run async
    CompletableFuture[] brFutures = erMap.values()
        .stream()
        .map(v -> CompletableFuture.runAsync(() -> interpretBrFn.accept(v), executor))
        .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(brFutures).get();

    return this;
  }

  // Filter GBIF id duplicates
  private Consumer<ExtendedRecord> filterByGbifId(Map<Long, BasicRecord> map, Map<String, BasicRecord> invalidMap) {
    return er ->
        basicTransform.processElement(er)
            .ifPresent(br -> {
              if (br.getGbifId() != null) {
                BasicRecord record = map.get(br.getGbifId());
                if (record != null) {
                  int compare = HashUtils.getSha1(br.getId()).compareTo(HashUtils.getSha1(record.getId()));
                  if (compare > 0) {
                    map.put(br.getGbifId(), br);
                    invalidMap.put(record.getId(), record);
                  }
                } else {
                  map.put(br.getGbifId(), br);
                }
              } else {
                invalidMap.put(br.getId(), br);
              }
            });
  }

}
