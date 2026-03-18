package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import jakarta.validation.constraints.NotNull;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.diagnostics.common.KeygenServiceFactory;
import org.gbif.pipelines.diagnostics.strategy.DeletionStrategy.DeletionStrategyType;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@Builder
public class RepairGbifIDLookupTool implements Tool {

  public enum FileType {
    DWCA,
    FILE
  }

  private long counter;

  @Parameter(names = "--tool")
  public CliTool tool;

  @Parameter(names = "--dataset-key", description = "GBIF registry ID for the dataset")
  @NotNull
  public String datasetKey;

  @Parameter(
      names = "--input-source",
      description = "Use DWCA archive as a lookup keys source, provide full input path")
  public File source;

  @Parameter(names = "--file-type", description = "Use DWCA or FILE with lookup keys")
  public FileType fileType;

  @Parameter(names = "--triplet-lookup-key", description = "Use single triplet as a lookup key")
  public String tripletLookupKey;

  @Parameter(
      names = "--occurrenceID-lookup-key",
      description = "Use single occurrenceID as a lookup key")
  public String occurrenceIdLookupKey;

  @Parameter(names = "--tmp")
  @NotNull
  @Builder.Default
  public File tmp = new File("/tmp/dwca-diagnostic-tool");

  @Parameter(names = "--zk-connection", description = "Zookeeper connection")
  public String zkConnection;

  @Parameter(names = "--hbase-znode", description = "Hbase zookeeper node name")
  public String hbaseZnode;

  @Parameter(names = "--lookup-table", description = "Hbase occurrence lookup table")
  @NotNull
  public String lookupTable;

  @Parameter(names = "--counter-table", description = "Hbase counter lookup table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occurrence-table", description = "Hbase occurrence table")
  @NotNull
  public String occurrenceTable;

  @Parameter(names = "--dry-run", description = "Prints messages, but skips delete method")
  @Builder.Default
  public boolean dryRun = false;

  @Parameter(
      names = "--deletion-strategy",
      description =
          "Which gbifID to delete? "
              + "MIN - compares gbifIDs for triplet and occurrenceID and deletes oldest. "
              + "MAX - compares gbifIDs for triplet and occurrenceID and deletes latest. "
              + "TRIPLET - Deletes gbifIDs for triplets. "
              + "OCCURRENCE_ID - Deletes gbifIDs for occurrenceIDs. "
              + "BOTH - Deletes gbifIDs for occurrenceIDs and triplets.")
  @NotNull
  public DeletionStrategyType deletionStrategyType;

  @Parameter(
      names = "--only-collisions",
      description = "Apply deletion strategy only for IDs with collisions")
  @Builder.Default
  public boolean onlyCollisions = false;

  @Parameter(names = "--help", description = "Display help information", order = 4)
  @Builder.Default
  public boolean help = false;

  @Builder.Default public Connection connection = null;

  @Override
  public boolean getHelp() {
    return help;
  }

  @Override
  public void check(JCommander jc) {
    boolean useTriple = tripletLookupKey != null && !tripletLookupKey.isEmpty();
    boolean useOccurrenceId = occurrenceIdLookupKey != null && !occurrenceIdLookupKey.isEmpty();
    boolean useDwcaDirectory = source != null && source.exists();

    checkArguments(
        jc,
        !useDwcaDirectory && !useTriple && !useOccurrenceId,
        "Lookup source is empty, use one of 3 variants");

    checkArguments(
        jc,
        useDwcaDirectory && (useTriple || useOccurrenceId),
        "Lookup source can't be dwca and triplet/occurrenceId");

    checkArguments(jc, deletionStrategyType == null, "--deletion-strategy can't be null");
    checkArguments(jc, lookupTable == null, "--lookup-table can't be null");
    checkArguments(jc, counterTable == null, "--counter-table can't be null");
    checkArguments(jc, occurrenceTable == null, "--occurrence-table can't be null");
    checkArguments(jc, zkConnection == null, "--zookeeper connection can't be null");
  }

  @Override
  public void run() {
    log.info(
        "Running diagnostic tool for - {}, using deletion strategy - {}",
        source,
        deletionStrategyType);

    HBaseLockingKeyService keygenService = null;

    try {
      keygenService =
          KeygenServiceFactory.builder()
              .zkConnection(zkConnection)
              .hbaseZnode(hbaseZnode)
              .lookupTable(lookupTable)
              .counterTable(counterTable)
              .occurrenceTable(occurrenceTable)
              .connection(connection)
              .datasetKey(datasetKey)
              .build()
              .create();

      if (source != null && fileType == FileType.DWCA) {
        runDwca(keygenService);
      } else if (source != null
          && fileType == FileType.FILE
          && (deletionStrategyType == DeletionStrategyType.OCCURRENCE_ID
              || deletionStrategyType == DeletionStrategyType.TRIPLET)) {
        runFile(keygenService);
      } else if (tripletLookupKey != null || occurrenceIdLookupKey != null) {
        runSingleLookup(keygenService);
      } else {
        log.warn("Settings are incorrect. Check all provided keys");
      }
    } finally {
      if (keygenService != null && connection == null) {
        keygenService.close();
      }
    }

    log.info("Finished. IDs with collisions: {}", counter);
  }

  private static void checkArguments(JCommander jc, boolean check, String message) {
    if (check) {
      log.error(message);
      jc.usage();
      throw new IllegalArgumentException(message);
    }
  }

  @SneakyThrows
  private void runDwca(HBaseLockingKeyService keygenService) {

    Archive dwca;
    if (source.isDirectory()) {
      dwca = DwcFiles.fromLocation(source.toPath());
    } else {
      Path t = tmp.toPath().resolve(datasetKey).resolve(UUID.randomUUID().toString());
      dwca = DwcFiles.fromCompressed(source.toPath(), t);
    }

    for (Record r : dwca.getCore()) {
      String ic = r.value(DwcTerm.institutionCode);
      String cc = r.value(DwcTerm.collectionCode);
      String cn = r.value(DwcTerm.catalogNumber);
      String occID = r.value(DwcTerm.occurrenceID);

      String triplet = OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);

      deleteKeys(keygenService, triplet, occID);
    }
  }

  @SneakyThrows
  private void runFile(HBaseLockingKeyService keygenService) {
    try (Stream<String> lines = Files.lines(source.toPath())) {
      lines.forEach(
          key -> {
            String occID = deletionStrategyType == DeletionStrategyType.OCCURRENCE_ID ? key : null;
            String triplet = deletionStrategyType == DeletionStrategyType.TRIPLET ? key : null;
            deleteKeys(keygenService, triplet, occID);
          });
    }
  }

  private void runSingleLookup(HBaseLockingKeyService keygenService) {
    deleteKeys(keygenService, tripletLookupKey, occurrenceIdLookupKey);
  }

  private void deleteKeys(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {
    Map<String, Long> keysToDelete =
        deletionStrategyType.getKeysToDelete(keygenService, onlyCollisions, triplet, occurrenceId);
    if (!keysToDelete.isEmpty()) {
      log.info("Use keys to request, triplet: {} and occurrenceId: {}", triplet, occurrenceId);
      String message = dryRun ? "Lookup key" : "Delete lookup key";
      keysToDelete.forEach((k, v) -> log.info("{} - {}, gbifID - {}", message, k, v));
      counter++;
    }
    if (!dryRun && !keysToDelete.isEmpty()) {
      keygenService.deleteKeyByUniques(keysToDelete.keySet());
    }
  }
}
