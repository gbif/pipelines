package org.gbif.pipelines.fragmenter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.stream.Collectors;

import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.fragmenter.common.FragmentsConfig;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RecordUnit;
import org.gbif.pipelines.fragmenter.common.RecordUnitConverter;
import org.gbif.pipelines.fragmenter.common.StarRecordCopy;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.utils.file.ClosableIterator;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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
  private Path tempDir;

  @NonNull
  private String datasetId;

  @NonNull
  private Integer attempt;

  @NonNull
  private String protocol;

  @NonNull
  Boolean useTriplet;

  @NonNull
  Boolean useOccurrenceId;

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
    AtomicInteger occurrenceCounter = new AtomicInteger(0);

    Queue<List<RecordUnit>> rows = new LinkedBlockingQueue<>();
    rows.add(new ArrayList<>(batchSize));

    Consumer<RecordUnit> addRowFn = er -> Optional.ofNullable(rows.peek()).ifPresent(req -> req.add(er));

    log.info("Uploadind fragments from {}", pathToArchive);
    try (Table table = connection.getTable(config.getTableName());
        ClosableIterator<StarRecord> starRecordIterator = readDwca();
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      Consumer<List<RecordUnit>> hbaseBulkFn = l -> {
        backPressureCounter.incrementAndGet();

        Map<String, String> map = RecordUnitConverter.convert(keygenService, validator, useTriplet, useOccurrenceId, l);
        HbaseStore.putRecords(table, datasetId, attempt, protocol, map);

        occurrenceCounter.addAndGet(map.size());
        backPressureCounter.decrementAndGet();
      };

      Runnable pushIntoHbaseFn = () ->
          Optional.ofNullable(rows.poll())
              .filter(req -> !req.isEmpty())
              .ifPresent(req -> {
                if (useSyncMode) {
                  hbaseBulkFn.accept(req);
                } else {
                  futures.add(CompletableFuture.runAsync(() -> hbaseBulkFn.accept(req), executor));
                }
              });

      // Read all records
      while (starRecordIterator.hasNext()) {

        while (backPressureCounter.get() > backPressure) {
          log.info("Back pressure barrier: too many rows wainting...");
          TimeUnit.MILLISECONDS.sleep(200L);
        }

        StarRecord starRecord = StarRecordCopy.create(starRecordIterator.next());
        convertToRecordUnits(starRecord).forEach(record -> {
          List<RecordUnit> peek = rows.peek();
          if (peek != null && peek.size() < batchSize - 1) {
            addRowFn.accept(record);
          } else {
            addRowFn.accept(record);
            pushIntoHbaseFn.run();
            rows.add(new ArrayList<>(batchSize));
          }
        });

      }

      // Final push
      pushIntoHbaseFn.run();

      // Wait for all async jobs
      if (!useSyncMode) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      }
    }

    return occurrenceCounter.get();

  }

  public void close() {
    try {
      hbaseConnection.close();
    } catch (IOException ex) {
      log.error("Can't close HBase connection", ex);
    }
  }

  private ClosableIterator<StarRecord> readDwca() throws IOException {
    if (pathToArchive.endsWith(".dwca")) {
      return DwcFiles.fromCompressed(pathToArchive, tempDir).iterator();
    } else {
      return DwcFiles.fromLocation(pathToArchive).iterator();
    }
  }

  private List<RecordUnit> convertToRecordUnits(StarRecord starRecord) {
    List<Record> records = starRecord.extension(DwcTerm.Occurrence);
    if (records == null || records.isEmpty()) {
      return Collections.singletonList(RecordUnit.create(starRecord));
    } else {
      return records.stream().map(r -> RecordUnit.create(starRecord.core(), r)).collect(Collectors.toList());
    }
  }

}
