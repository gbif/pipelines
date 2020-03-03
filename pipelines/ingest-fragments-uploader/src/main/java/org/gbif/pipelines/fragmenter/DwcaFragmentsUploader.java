package org.gbif.pipelines.fragmenter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.fragmenter.common.FragmentsConfig;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.Keygen;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.core.converters.ExtendedRecordConverter.RECORD_ID_ERROR;
import static org.gbif.pipelines.fragmenter.common.Keygen.ERROR_KEY;

@Slf4j
@Builder
public class DwcaFragmentsUploader {

  @NonNull
  private FragmentsConfig config;

  @NonNull
  private KeygenConfig keygenConfig;

  @NonNull
  private Path pathToArchive;

  @NonNull
  private String datasetId;

  @NonNull
  private Integer attempt;

  @Builder.Default
  private int batchSize = 10;

  @Builder.Default
  private boolean useSyncMode = true;

  @Builder.Default
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Builder.Default
  private int backPressure = 5;

  private Connection hbaseConnection;

  @SneakyThrows
  public long upload() {

    Connection connection = Optional.ofNullable(hbaseConnection)
        .orElse(HbaseConnectionFactory.getInstance(keygenConfig.getHbaseZk()).getConnection());
    HBaseLockingKeyService keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger backPressureCounter = new AtomicInteger(0);

    Queue<List<ExtendedRecord>> rows = new LinkedBlockingQueue<>();
    rows.add(new ArrayList<>(batchSize));

    Consumer<ExtendedRecord> addRowFn = er -> Optional.ofNullable(rows.peek()).ifPresent(req -> req.add(er));

    DwcaReader reader = DwcaReader.fromLocation(pathToArchive.toString());
    log.info("Uploadind fragments from {}", pathToArchive);

    try (Table table = connection.getTable(config.getTableName())) {

      Consumer<List<ExtendedRecord>> hbaseBulkFn = list -> {
        backPressureCounter.incrementAndGet();

        Map<Long, String> map = convert(keygenService, list);
        HbaseStore.putList(table, datasetId, attempt, map);

        backPressureCounter.decrementAndGet();
      };

      Runnable pushIntoHbaseFn = () -> Optional.ofNullable(rows.poll())
          .filter(req -> !req.isEmpty())
          .ifPresent(req -> {
            if (useSyncMode) {
              hbaseBulkFn.accept(req);
            } else {
              futures.add(CompletableFuture.runAsync(() -> hbaseBulkFn.accept(req), executor));
            }
          });

      // Read all records
      while (reader.advance()) {

        while (backPressureCounter.get() > backPressure) {
          log.info("Back pressure barrier: too many rows wainting...");
          TimeUnit.MILLISECONDS.sleep(200L);
        }

        ExtendedRecord record = reader.getCurrent();
        if (!record.getId().equals(RECORD_ID_ERROR)) {

          List<ExtendedRecord> peek = rows.peek();
          if (peek != null && peek.size() < batchSize - 1) {
            addRowFn.accept(record);
          } else {
            addRowFn.accept(record);
            pushIntoHbaseFn.run();
            rows.add(new ArrayList<>(batchSize));
          }

        }
      }
      reader.close();

      // Final push
      pushIntoHbaseFn.run();

      // Wait for all async jobs
      if (!useSyncMode) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      }
    }

    return reader.getRecordsReturned();

  }

  private Map<Long, String> convert(HBaseLockingKeyService keygenService, List<ExtendedRecord> erList) {

    Function<ExtendedRecord, Long> keyFn = er -> Keygen.getKey(keygenService, er);
    Function<ExtendedRecord, String> valueFn = er -> {
      // TODO: IMPLEMENT PROPER MAPPER!
      return er.toString();
    };

    Map<Long, String> result = erList.stream().collect(Collectors.toMap(keyFn, valueFn, (s, s2) -> s));
    result.remove(ERROR_KEY);
    return result;
  }

}
