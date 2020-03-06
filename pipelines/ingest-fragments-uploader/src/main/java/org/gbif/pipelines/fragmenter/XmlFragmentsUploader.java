package org.gbif.pipelines.fragmenter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.pipelines.fragmenter.common.RecordUnit;
import org.gbif.pipelines.fragmenter.common.RecordUnitConverter;
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
public class XmlFragmentsUploader {

  private static final String EXT_RESPONSE = ".response";
  private static final String EXT_XML = ".xml";

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

    try (Table table = connection.getTable(TableName.valueOf(tableName));
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

      File inputFile = ParserFileUtils.uncompressAndGetInputFile(pathToArchive.toString());
      for (File f : getInputFiles(inputFile)) {

        while (backPressureCounter.get() > backPressure) {
          log.info("Back pressure barrier: too many rows wainting...");
          TimeUnit.MILLISECONDS.sleep(200L);
        }

        OccurrenceParser.parse(f).stream()
            .map(RecordUnit::create)
            .forEach(record -> {
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

  /** Traverse the input directory and gets all the files. */
  private List<File> getInputFiles(File inputhFile) throws IOException {
    Predicate<Path> prefixPr = x -> x.toString().endsWith(EXT_RESPONSE) || x.toString().endsWith(EXT_XML);
    try (Stream<Path> walk = Files.walk(inputhFile.toPath())
        .filter(file -> file.toFile().isFile() && prefixPr.test(file))) {
      return walk.map(Path::toFile).collect(Collectors.toList());
    }
  }

}
