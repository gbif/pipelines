package org.gbif.pipelines.diagnostics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.diagnostics.strategy.DeletionStrategy.DeletionStrategyType;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class DiagnosticTool {

  @Parameter(names = "--datase-key", description = "GBIF registry ID for the dataset")
  @NotNull
  public String datasetKey;

  @Parameter(
      names = "--input-directory",
      description = "Use DWCA archive as a lookup keys source, provide full input path")
  public File dwcaDirectory;

  @Parameter(names = "--triplet-lookup-key", description = "Use single triplet as a lookup key")
  public String tripletLookupKey;

  @Parameter(
      names = "--occurrenceID-lookup-key",
      description = "Use single occurrenceID as a lookup key")
  public String occurrenceIdLookupKey;

  @Parameter(names = "--tmp")
  @NotNull
  public File tmp = new File("/tmp/dwca-diagnostic-tool");

  @Parameter(names = "--zk-connection", description = "Zookeeper connection")
  @NotNull
  public String zkConnection;

  @Parameter(names = "--lookup-table", description = "Occurrence lookup table")
  @NotNull
  public String lookupTable;

  @Parameter(
      names = "--deletion-strategy",
      description =
          "Which gbifID to delete? "
              + "min - compares gbifIDs for triplet and occurrenceID and deletes oldest. "
              + "max - compares gbifIDs for triplet and occurrenceID and deletes latest. "
              + "triplet - Deletes gbifIDs for triplets. "
              + "occurrenceID - Deletes gbifIDs for occurrenceIDs. "
              + "all - Deletes gbifIDs for occurrenceIDs and triplets.")
  @NotNull
  public DeletionStrategyType deletionStrategyType;

  @Parameter(
      names = "--only-collisions",
      description = "Apply deletion strategy only for IDs with collisions")
  @NotNull
  public Boolean onlyCollisions;

  @Parameter(names = "--help", description = "Display help information", order = 4)
  public boolean help = false;

  public static void main(String... argv) {
    DiagnosticTool main = new DiagnosticTool();
    JCommander jc = JCommander.newBuilder().addObject(main).build();
    jc.parse(argv);
    if (main.help) {
      jc.usage();
    }

    boolean useTriple = main.tripletLookupKey != null && !main.tripletLookupKey.isEmpty();
    boolean useOccurrenceId =
        main.occurrenceIdLookupKey != null && !main.occurrenceIdLookupKey.isEmpty();
    boolean useDwcaDirectory = main.dwcaDirectory != null && main.dwcaDirectory.exists();

    if (!useDwcaDirectory && !useTriple && !useOccurrenceId) {
      log.error("Lookup source is empty, use one of 3 variants");
      jc.usage();
      System.exit(-1);
    }

    if (useDwcaDirectory && (useTriple || useOccurrenceId)) {
      log.error("Lookup source can't be dwca and triplet/occurrenceId");
      jc.usage();
      System.exit(-1);
    }

    Objects.requireNonNull(
        main.deletionStrategyType, "--deletion-strategy can't be null, use --help");
    Objects.requireNonNull(main.zkConnection, "--zk-connection can't be null, use --help");
    Objects.requireNonNull(main.lookupTable, "--lookup-table can't be null, use --help");
    Objects.requireNonNull(main.onlyCollisions, "--only-collisions can't be null, use --help");

    main.run();
  }

  public void run() {
    log.info(
        "Running diagnostic tool for - {}, using deletion strategy - {}",
        dwcaDirectory,
        deletionStrategyType);

    KeygenConfig cfg =
        KeygenConfig.builder().zkConnectionString(zkConnection).lookupTable(lookupTable).create();

    Connection connection = HbaseConnectionFactory.getInstance(zkConnection).getConnection();
    HBaseLockingKeyService keygenService = new HBaseLockingKeyService(cfg, connection, datasetKey);

    if (dwcaDirectory != null) {
      runDwca(keygenService);
    } else {
      runSingleLookup(keygenService);
    }
  }

  @SneakyThrows
  public void runDwca(HBaseLockingKeyService keygenService) {

    Archive dwca = DwcFiles.fromCompressed(dwcaDirectory.toPath(), tmp.toPath());
    for (Record r : dwca.getCore()) {
      String ic = r.value(DwcTerm.institutionCode);
      String cc = r.value(DwcTerm.collectionCode);
      String cn = r.value(DwcTerm.catalogNumber);
      String occID = r.value(DwcTerm.occurrenceID);

      String triplet = OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);

      Set<String> keysToDelete =
          deletionStrategyType.getKeysToDelete(keygenService, onlyCollisions, triplet, occID);

      keysToDelete.forEach(k -> log.info("Delete lookup key - {}", k));
      keygenService.deleteKeyByUniques(keysToDelete);
    }
  }

  public void runSingleLookup(HBaseLockingKeyService keygenService) {
    Set<String> keysToDelete =
        deletionStrategyType.getKeysToDelete(
            keygenService, onlyCollisions, tripletLookupKey, occurrenceIdLookupKey);

    keysToDelete.forEach(k -> log.info("Delete lookup key - {}", k));
    keygenService.deleteKeyByUniques(keysToDelete);
  }
}
