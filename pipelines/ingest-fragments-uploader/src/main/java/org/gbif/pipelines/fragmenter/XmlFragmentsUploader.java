package org.gbif.pipelines.fragmenter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.fragmenter.common.FragmentsUploader;
import org.gbif.pipelines.fragmenter.common.HbaseConfiguration;
import org.gbif.pipelines.fragmenter.habse.FragmentRow;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class XmlFragmentsUploader implements FragmentsUploader {

  private static final String EXT_RESPONSE = ".response";
  private static final String EXT_XML = ".xml";

  @NonNull
  private HbaseConfiguration config;

  @NonNull
  private KeygenConfig keygenConfig;

  @NonNull
  private Path pathToArchive;

  @NonNull
  private String datasetId;

  @Builder.Default
  private boolean useSyncMode = false;

  @Builder.Default
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Builder.Default
  private int backPressure = 5;

  @SneakyThrows
  @Override
  public long upload() {

    Connection connection = HbaseConnectionFactory.getInstance(keygenConfig.getHbaseZk()).getConnection();
    HBaseLockingKeyService keygenService = new HBaseLockingKeyService(keygenConfig, connection, datasetId);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger backPressureCounter = new AtomicInteger(0);
    AtomicInteger occurrenceCounter = new AtomicInteger(0);

    Consumer<List<Row>> hbaseBulkFn = list -> {
      backPressureCounter.incrementAndGet();
      // TODO: Push into Hbase
      occurrenceCounter.addAndGet(list.size());
      backPressureCounter.decrementAndGet();
    };

    Consumer<List<Row>> pushIntoHbaseFn = list ->
        Optional.ofNullable(list)
            .filter(req -> !req.isEmpty())
            .ifPresent(req -> {
              if (useSyncMode) {
                hbaseBulkFn.accept(req);
              } else {
                futures.add(CompletableFuture.runAsync(() -> hbaseBulkFn.accept(req), executor));
              }
            });

    try (UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      File inputFile = ParserFileUtils.uncompressAndGetInputFile(pathToArchive.toString());
      for (File f : getInputFiles(inputFile)) {

        while (backPressureCounter.get() > backPressure) {
          log.info("Back pressure barrier: too many rows wainting...");
          TimeUnit.MILLISECONDS.sleep(200L);
        }

        List<RawXmlOccurrence> xmlOccurrenceList = new OccurrenceParser().parseFile(f);
        List<Row> rowList = convert(keygenService, validator, xmlOccurrenceList);
        pushIntoHbaseFn.accept(rowList);
      }

    }

    // Wait for all futures
    if (!useSyncMode) {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }

    return occurrenceCounter.get();
  }

  private List<Row> convert(HBaseLockingKeyService keygenService, UniquenessValidator validator,
      List<RawXmlOccurrence> rawXmlOccurrences) {
    return rawXmlOccurrences.stream()
        .map(x -> FragmentRow.create("", ""))
        .collect(Collectors.toList());
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
