package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;

/**
 * Diagnostic tool to print out the values in occurrenceID and the triplet in each crawl attempt
 * DwC-A. Example usage to print all records for a dataset:
 *
 * <pre>
 * java RecordIDsByCrawlAttempt --catalogNumber "PERTH 3994120" \
 * --occurrenceID "01c2a4e5-eee9-43c5-a2e8-da562711e9b8" \
 * --directory /crawler/dwca/dataset1 --tmp /tmp/scratch
 * </pre>
 */
@Slf4j
@Builder
public class RecordIDsByCrawlAttemptTool implements Tool {
  private static final String ARCHIVE_REGEX = ".{36}.[0-9]*.dwca"; // filename format of attempts

  @Parameter(names = "--tool")
  public CliTool tool;

  @Parameter(names = "--directory-path")
  @NotNull
  public File directory;

  @Parameter(names = "--tmp")
  @NotNull
  @Builder.Default
  public File tmp = new File("/tmp/delete-me");

  @Parameter(names = "--catalogNumber")
  public String catalogNumber;

  @Parameter(names = "--occurrenceID")
  public String occurrenceID;

  @Parameter(names = "--help", description = "Display help information", order = 4)
  @Builder.Default
  public boolean help = false;

  @Override
  public boolean getHelp() {
    return help;
  }

  @Override
  public void run() {
    // filter out symlinks and other files
    File[] archives =
        directory.listFiles(
            f ->
                f.isFile()
                    && !Files.isSymbolicLink(f.toPath())
                    && f.getName().matches(ARCHIVE_REGEX));

    if (archives == null) {
      log.error("Archives are empty or null");
      System.exit(-1);
    }

    // sort by the attempt numerically
    Arrays.sort(archives, Comparator.comparingInt(RecordIDsByCrawlAttemptTool::attemptFromName));

    // System console allowing to read or pipe to other processes
    System.out.printf(
        "%s\t%s\t%s\t%s\t%s%n",
        "Attempt", "institutionCode", "collectionCode", "catalogNumber", "occurrenceID");
    Arrays.asList(archives).forEach(this::print);
  }

  private static int attemptFromName(File file) {
    return Integer.parseInt(file.getName().substring(37).replace(".dwca", ""));
  }

  @SneakyThrows
  private void print(File a) {
    Archive dwca = DwcFiles.fromCompressed(a.toPath(), tmp.toPath());
    for (Record r : dwca.getCore()) {
      String cn = r.value(DwcTerm.catalogNumber);
      String occID = r.value(DwcTerm.occurrenceID);
      if ((cn != null && cn.equals(catalogNumber))
          || (occID != null && occID.equals(occurrenceID))) {

        System.out.printf(
            "%d\t%s\t%s\t%s\t%s%n",
            attemptFromName(a),
            r.value(DwcTerm.institutionCode),
            r.value(DwcTerm.collectionCode),
            r.value(DwcTerm.catalogNumber),
            r.value(DwcTerm.occurrenceID));
      }
    }
  }
}
