package org.gbif.pipelines.diagnostics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import javax.validation.constraints.NotNull;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.utils.file.ClosableIterator;

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
public class RecordIDsByCrawlAttempt {
  private static final String ARCHIVE_REGEX = ".{36}.[0-9]*.dwca"; // filename format of attempts

  @Parameter(names = "--directory")
  @NotNull
  public File directory;

  @Parameter(names = "--tmp")
  @NotNull
  public File tmp = new File("/tmp/delme");

  @Parameter(names = "--catalogNumber")
  public String catalogNumber;

  @Parameter(names = "--occurrenceID")
  public String occurrenceID;

  public static void main(String... argv) {
    RecordIDsByCrawlAttempt main = new RecordIDsByCrawlAttempt();
    JCommander.newBuilder().addObject(main).build().parse(argv);
    main.run();
  }

  public void run() {
    // filter out symlinks and other files
    File[] archives =
        directory.listFiles(
            f ->
                f.isFile()
                    && !Files.isSymbolicLink(f.toPath())
                    && f.getName().matches(ARCHIVE_REGEX));

    // sort by the attempt numerically
    Arrays.sort(archives, Comparator.comparingInt(RecordIDsByCrawlAttempt::attemptFromName));

    // System console allowing to read or pipe to other processes
    System.out.println(
        String.format(
            "%s\t%s\t%s\t%s\t%s",
            "Attempt", "institutionCode", "collectionCode", "catalogNumber", "occurrenceID"));
    Arrays.asList(archives)
        .forEach(
            a -> {
              try {
                Archive dwca = DwcFiles.fromCompressed(a.toPath(), tmp.toPath());
                ClosableIterator<Record> iter = dwca.getCore().iterator();
                while (iter.hasNext()) {
                  Record r = iter.next();
                  String cn = r.value(DwcTerm.catalogNumber);
                  String occID = r.value(DwcTerm.occurrenceID);
                  if ((cn != null && cn.equals(catalogNumber))
                      || (occID != null && occID.equals(occurrenceID))) {

                    System.out.println(
                        String.format(
                            "%d\t%s\t%s\t%s\t%s",
                            attemptFromName(a),
                            r.value(DwcTerm.institutionCode),
                            r.value(DwcTerm.collectionCode),
                            r.value(DwcTerm.catalogNumber),
                            r.value(DwcTerm.occurrenceID)));
                  }
                }

              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static int attemptFromName(File file) {
    return Integer.parseInt(file.getName().substring(37).replace(".dwca", ""));
  }
}
