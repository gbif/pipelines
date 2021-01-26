package org.gbif.pipelines.diagnostics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.util.Set;
import javax.validation.constraints.NotNull;
import lombok.Builder;
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

/**
 * Diagnostic tool to print out the values in occurrenceID and the triplet in each crawl attempt
 * DwC-A. Example usage to print all records for a dataset:
 */
@Slf4j
@Builder
public class RepairGbifIDLookupTool {

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
  @Builder.Default
  public File tmp = new File("/tmp/dwca-diagnostic-tool");

  @Parameter(names = "--zk-connection", description = "Zookeeper connection")
  public String zkConnection;

  @Parameter(names = "--lookup-table", description = "Hbase occurrence lookup table")
  @NotNull
  public String lookupTable;

  @Parameter(names = "--counter-table", description = "Hbase counter lookup table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occurrence-table", description = "Hbase occurrence table")
  @NotNull
  public String occurrenceTable;

  @Parameter(
      names = "--deletion-strategy",
      description =
          "Which gbifID to delete? "
              + "min - compares gbifIDs for triplet and occurrenceID and deletes oldest. "
              + "max - compares gbifIDs for triplet and occurrenceID and deletes latest. "
              + "triplet - Deletes gbifIDs for triplets. "
              + "occurrenceID - Deletes gbifIDs for occurrenceIDs. "
              + "both - Deletes gbifIDs for occurrenceIDs and triplets.")
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

  public static void main(String... argv) {
    RepairGbifIDLookupTool main = RepairGbifIDLookupTool.builder().build();
    JCommander jc = JCommander.newBuilder().addObject(main).build();
    jc.parse(argv);
    if (main.help) {
      jc.usage();
      System.exit(0);
    }

    boolean useTriple = main.tripletLookupKey != null && !main.tripletLookupKey.isEmpty();
    boolean useOccurrenceId =
        main.occurrenceIdLookupKey != null && !main.occurrenceIdLookupKey.isEmpty();
    boolean useDwcaDirectory = main.dwcaDirectory != null && main.dwcaDirectory.exists();

    checkArguments(
        jc,
        !useDwcaDirectory && !useTriple && !useOccurrenceId,
        "Lookup source is empty, use one of 3 variants");

    checkArguments(
        jc,
        useDwcaDirectory && (useTriple || useOccurrenceId),
        "Lookup source can't be dwca and triplet/occurrenceId");

    checkArguments(jc, main.deletionStrategyType == null, "--deletion-strategy can't be null");
    checkArguments(jc, main.lookupTable == null, "--lookup-table can't be null");
    checkArguments(jc, main.counterTable == null, "--counter-table can't be null");
    checkArguments(jc, main.occurrenceTable == null, "--occurrence-table can't be null");
    checkArguments(jc, main.zkConnection == null, "--zookeeper connection can't be null");

    main.run();
  }

  public void run() {
    log.info(
        "Running diagnostic tool for - {}, using deletion strategy - {}",
        dwcaDirectory,
        deletionStrategyType);

    KeygenConfig cfg =
        KeygenConfig.builder()
            .zkConnectionString(zkConnection)
            .lookupTable(lookupTable)
            .counterTable(counterTable)
            .occurrenceTable(occurrenceTable)
            .create();

    if (connection == null) {
      connection = HbaseConnectionFactory.getInstance(zkConnection).getConnection();
    }
    HBaseLockingKeyService keygenService = new HBaseLockingKeyService(cfg, connection, datasetKey);

    if (dwcaDirectory != null) {
      runDwca(keygenService);
    } else {
      runSingleLookup(keygenService);
    }
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
    if (dwcaDirectory.isDirectory()) {
      dwca = DwcFiles.fromLocation(dwcaDirectory.toPath());
    } else {
      dwca = DwcFiles.fromCompressed(dwcaDirectory.toPath(), tmp.toPath());
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

  private void runSingleLookup(HBaseLockingKeyService keygenService) {
    deleteKeys(keygenService, tripletLookupKey, occurrenceIdLookupKey);
  }

  private void deleteKeys(
      HBaseLockingKeyService keygenService, String triplet, String occurrenceId) {
    Set<String> keysToDelete =
        deletionStrategyType.getKeysToDelete(keygenService, onlyCollisions, triplet, occurrenceId);
    keysToDelete.forEach(k -> log.info("Delete lookup key - {}", k));
    keygenService.deleteKeyByUniques(keysToDelete);
  }
}
