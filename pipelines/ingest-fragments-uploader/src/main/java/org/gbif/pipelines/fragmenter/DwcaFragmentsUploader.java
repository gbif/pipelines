package org.gbif.pipelines.fragmenter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.fragmenter.common.FragmentsUploader;
import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.hadoop.hbase.client.Row;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.core.converters.ExtendedRecordConverter.RECORD_ID_ERROR;

@Slf4j
@Builder
public class DwcaFragmentsUploader implements FragmentsUploader {

  @NonNull
  private HbaseConfiguration config;

  @NonNull
  private Path pathToArchive;

  @Builder.Default
  private int batchSize = 10;

  @Builder.Default
  private boolean useSyncMode;

  @Builder.Default
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Builder.Default
  private int backPressure = 5;

  @SneakyThrows
  @Override
  public long upload() {

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger backPressureCounter = new AtomicInteger(0);

    Queue<List<Row>> rows = new LinkedBlockingQueue<>();
    rows.add(new ArrayList<>(batchSize));

    Function<ExtendedRecord, Row> erToRowFn = er -> {
      // TODO: CONVERTER TO ROW
      return null;
    };

    Consumer<ExtendedRecord> addRowFn = br -> Optional.ofNullable(rows.peek())
        .ifPresent(req -> req.add(erToRowFn.apply(br)));

    DwcaReader reader = DwcaReader.fromLocation(pathToArchive.toString());
    log.info("Uploadind fragments from {}", pathToArchive);

    Consumer<List<Row>> hbaseBulkFn = list -> {
      backPressureCounter.incrementAndGet();
      // TODO: Push into Hbase
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

        List<Row> peek = rows.peek();
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

    // Wait for all futures
    if (!useSyncMode) {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    return reader.getRecordsReturned();

  }

}
