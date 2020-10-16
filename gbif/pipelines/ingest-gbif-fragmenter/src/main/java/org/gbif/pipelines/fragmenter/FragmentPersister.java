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
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.api.vocabulary.EndpointType;
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

/**
 * FragmentsUploader reads dwca/xml based archive and uploads raw json/xml records into HBase table.
 *
 * <p>Processing workflow: 1. Read a dwca/xml archive 2. Collect raw records into small batches
 * (batch size is configurable) 3. Get or create GBIF id for each element of the batch and create
 * keys (salt + ":" + GBIF id) 4. Get **dateCreated** from the table using GBIF id, if a record is
 * exist 5. Create HBase put(create new or update existing) records and upload them into HBase
 *
 * <pre>{@code
 * long recordsProcessed = FragmentsUploader.dwcaBuilder()
 *      .tableName("Tabe name")
 *      .keygenConfig(config)
 *      .pathToArchive(path)
 *      .useTriplet(true)
 *      .useOccurrenceId(true)
 *      .datasetKey(datasetKey)
 *      .attempt(attempt)
 *      .endpointType(EndpointType.DWC_ARCHIVE)
 *      .hbaseConnection(connection)
 *      .build()
 *      .upload();
 * }</pre>
 */
@Slf4j
@Builder
public class FragmentPersister {

  @NonNull private Strategy strategy;

  @NonNull private String tableName;

  @NonNull private KeygenConfig keygenConfig;

  @NonNull private Path pathToArchive;

  @NonNull private String datasetKey;

  @NonNull private Integer attempt;

  @NonNull private EndpointType endpointType;

  @NonNull private Boolean useTriplet;

  @NonNull private Boolean useOccurrenceId;

  @Builder.Default private int batchSize = 100;

  @Builder.Default private boolean useSyncMode = true;

  @Builder.Default private ExecutorService executor = Executors.newSingleThreadExecutor();

  private Integer backPressure;

  private Connection hbaseConnection;

  public static FragmentPersisterBuilder xmlBuilder() {
    return FragmentPersister.builder().strategy(XmlStrategy.create());
  }

  public static FragmentPersisterBuilder dwcaBuilder() {
    return FragmentPersister.builder().strategy(DwcaStrategy.create());
  }

  @SneakyThrows
  public long persist() {

    // Init values
    final Phaser phaser = new Phaser(1);
    final AtomicInteger occurrenceCounter = new AtomicInteger(0);
    final Queue<List<OccurrenceRecord>> rows = new LinkedBlockingQueue<>();
    final Consumer<OccurrenceRecord> addRowFn =
        r -> Optional.ofNullable(rows.peek()).ifPresent(req -> req.add(r));
    final Connection connection =
        Optional.ofNullable(hbaseConnection)
            .orElse(
                HbaseConnectionFactory.getInstance(keygenConfig.getZkConnectionString())
                    .getConnection());
    final HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(keygenConfig, connection, datasetKey);

    rows.add(new ArrayList<>(batchSize));

    log.info("Uploadind fragments from {}", pathToArchive);
    try (Table table = connection.getTable(TableName.valueOf(tableName));
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      // Main function receives batch and puts it into HBase table
      Consumer<List<OccurrenceRecord>> hbaseBulkFn =
          l -> {
            Map<String, String> map =
                OccurrenceRecordConverter.convert(
                    keygenService, validator, useTriplet, useOccurrenceId, l);
            HbaseStore.putRecords(table, datasetKey, attempt, endpointType, map);

            int recordsReturned = occurrenceCounter.addAndGet(map.size());
            if (recordsReturned % 10_000 == 0) {
              log.info("{}_{}: Pushed [{}] records", datasetKey, attempt, recordsReturned);
            }
            phaser.arriveAndDeregister();
          };

      // Function gets a batch and pushed using sync or async way
      Runnable pushIntoHbaseFn =
          () ->
              Optional.ofNullable(rows.poll())
                  .filter(req -> !req.isEmpty())
                  .ifPresent(
                      req -> {
                        phaser.register();
                        if (useSyncMode) {
                          hbaseBulkFn.accept(req);
                        } else {
                          CompletableFuture.runAsync(() -> hbaseBulkFn.accept(req), executor);
                        }
                      });

      // Function accumulates and pushes batches
      Consumer<OccurrenceRecord> batchAndPushFn =
          record -> {
            checkBackpressure(phaser);
            addRowFn.accept(record);
            List<OccurrenceRecord> peek = rows.peek();
            if (peek == null || peek.size() > batchSize - 1) {
              pushIntoHbaseFn.run();
              rows.add(new ArrayList<>(batchSize));
            }
          };

      strategy.process(pathToArchive, batchAndPushFn);

      // Final push
      pushIntoHbaseFn.run();

      // Wait for all async jobs
      phaser.arriveAndAwaitAdvance();
    }

    return occurrenceCounter.get();
  }

  /** Close HBase connection */
  public void close() {
    try {
      hbaseConnection.close();
    } catch (IOException ex) {
      log.error("Can't close HBase connection", ex);
    }
  }

  /**
   * If the mode is async, check back pressure, the number of running async tasks must be less than
   * backPressure setting
   */
  private void checkBackpressure(Phaser phaser) {
    if (!useSyncMode && backPressure != null && backPressure > 0) {
      while (phaser.getUnarrivedParties() > backPressure) {
        log.debug("Back pressure barrier: pushing too much data, waiting...");
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
