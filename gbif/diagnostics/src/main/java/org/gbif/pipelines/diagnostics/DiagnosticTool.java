package org.gbif.pipelines.diagnostics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.diagnostics.strategy.DeletionStrategy.DeletionStrategyType;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.Objects;

@Slf4j
public class DiagnosticTool {

  @Parameter(names = "--datase-key", description = "GBIF registry ID for the dataset")
  @NotNull
  public String datasetKey;

  @Parameter(names = "--input-directory", description = "DWCA archive full input path")
  @NotNull
  public File directory;

  @Parameter(names = "--tmp")
  @NotNull
  public File tmp = new File("/tmp/dwca-diagnostic-tool");

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

  @Parameter(names = "--help", description = "Display help information", order = 4)
  public boolean help = false;

  public static void main(String... argv) {
    DiagnosticTool main = new DiagnosticTool();
    JCommander jc = JCommander.newBuilder().addObject(main).build();
    jc.parse(argv);
    if (main.help) {
      jc.usage();
    }
    Objects.requireNonNull(main.directory, "--input-directory can't be null");
    Objects.requireNonNull(main.deletionStrategyType, "--deletion-strategy can't be null");
    main.run();
  }

  @SneakyThrows
  public void run() {
    log.info(
        "Running diagnostic tool for - {}, using deletion strategy - {}",
        directory,
        deletionStrategyType);

    Archive dwca = DwcFiles.fromCompressed(directory.toPath(), tmp.toPath());

    HBaseLockingKeyService keygenService = null; // Init

    for (Record r : dwca.getCore()) {
      String ic = r.value(DwcTerm.institutionCode);
      String cc = r.value(DwcTerm.collectionCode);
      String cn = r.value(DwcTerm.catalogNumber);
      String occID = r.value(DwcTerm.occurrenceID);

      String triplet = OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);

      deletionStrategyType.deleteKeys(keygenService, triplet, occID);
    }
  }
}
