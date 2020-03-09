package org.gbif.pipelines.fragmenter;

import java.io.IOException;
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
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecordConverter;
import org.gbif.pipelines.fragmenter.strategy.DwcaStrategy;
import org.gbif.pipelines.fragmenter.strategy.Strategy;
import org.gbif.pipelines.fragmenter.strategy.XmlStrategy;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class FragmentsUploader {

  @NonNull
  private Strategy strategy;

  @NonNull
  private String tableName;

  @NonNull
  private KeygenConfig keygenConfig;

  @NonNull
  private Path pathToArchive;

  @NonNull
  private String datasetId;

  @NonNull
  private Integer attempt;

  @NonNull
  private String protocol;

  @NonNull
  private Boolean useTriplet;

  @NonNull
  private Boolean useOccurrenceId;

  @Builder.Default
  private int batchSize = 100;

  @Builder.Default
  private boolean useSyncMode = true;

  @Builder.Default
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  private Integer backPressure;

  private Connection hbaseConnection;

  public static FragmentsUploaderBuilder xmlBuilder() {
    return FragmentsUploader.builder().strategy(XmlStrategy.create());
  }

  public static FragmentsUploaderBuilder dwcaBuilder() {
    return FragmentsUploader.builder().strategy(DwcaStrategy.create());
  }

  @SneakyThrows
  public long upload() {

    final Phaser phaser = new Phaser(1);
    final AtomicInteger occurrenceCounter = new AtomicInteger(0);
    final Queue<List<OccurrenceRecord>> rows = new LinkedBlockingQueue<>();
    final Consumer<OccurrenceRecord> addRowFn = r -> Optional.ofNullable(rows.peek()).ifPresent(req -> req.add(r));
    final Connection connection = Optional.ofNullable(hbaseConnection)
        .orElse(HbaseConnectionFactory.getInstance(keygenConfig.getHbaseZk()).getConnection());
    final HBaseLockingKeyService keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);

    rows.add(new ArrayList<>(batchSize));

    log.info("Uploadind fragments from {}", pathToArchive);
    try (Table table = connection.getTable(TableName.valueOf(tableName));
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      Consumer<List<OccurrenceRecord>> hbaseBulkFn = l -> {

        Map<String, String> map =
            OccurrenceRecordConverter.convert(keygenService, validator, useTriplet, useOccurrenceId, l);
        HbaseStore.putRecords(table, datasetId, attempt, protocol, map);

        occurrenceCounter.addAndGet(map.size());
        phaser.arriveAndDeregister();
      };

      Runnable pushIntoHbaseFn = () ->
          Optional.ofNullable(rows.poll())
              .filter(req -> !req.isEmpty())
              .ifPresent(req -> {
                phaser.register();
                if (useSyncMode) {
                  hbaseBulkFn.accept(req);
                } else {
                  CompletableFuture.runAsync(() -> hbaseBulkFn.accept(req), executor);
                }
              });

      Consumer<OccurrenceRecord> pushRecordFn = record -> {
        checkBackpressure(phaser);
        List<OccurrenceRecord> peek = rows.peek();
        addRowFn.accept(record);
        if (peek == null || peek.size() < batchSize - 1) {
          pushIntoHbaseFn.run();
          rows.add(new ArrayList<>(batchSize));
        }
      };

      strategy.process(pathToArchive, pushRecordFn);

      // Final push
      pushIntoHbaseFn.run();

      // Wait for all async jobs
      phaser.arriveAndAwaitAdvance();
    }

    return occurrenceCounter.get();

  }

  /**
   * Close HBase connection
   */
  public void close() {
    try {
      hbaseConnection.close();
    } catch (IOException ex) {
      log.error("Can't close HBase connection", ex);
    }
  }

  /**
   * If the mode is async, check back pressure, the number of running async tasks must be less than backPressure setting
   */
  private void checkBackpressure(Phaser phaser) {
    if (!useSyncMode && backPressure != null && backPressure > 0) {
      while (phaser.getUnarrivedParties() > backPressure) {
        log.info("Back pressure barrier: too many rows wainting...");
        try {
          TimeUnit.MILLISECONDS.sleep(200L);
        } catch (InterruptedException ex) {
          log.warn("Back pressure barrier", ex);
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
