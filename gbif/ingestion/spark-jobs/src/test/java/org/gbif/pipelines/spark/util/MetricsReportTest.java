package org.gbif.pipelines.spark.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.IdentifiersPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MetricsReportTest {

  private static final String DATASET_A = "11111111-1111-1111-1111-111111111111";
  private static final String DATASET_B = "22222222-2222-2222-2222-222222222222";
  private static final String DATASET_C = "33333333-3333-3333-3333-333333333333";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void extensionCountsAreReportedAsColumns() throws Exception {
    File output = tmp.newFolder("output");

    // A: two extensions, B: one other extension, C: no archive-to-verbatim.yml
    writeMetrics(
        output, DATASET_A, IdentifiersPipeline.METRICS_FILENAME, "uniqueIdsCountAttempted: 5\n");
    writeMetrics(
        output,
        DATASET_A,
        MetricsReport.ARCHIVE_TO_VERBATIM_FILENAME,
        "archiveToErCount: 5\n"
            + "archiveToOccurrenceCount: 5\n"
            + "archiveToLargestFileCount: 7\n"
            + "extension_rs_Multimedia: 7\n"
            + "extension_rs_Identifier: 2\n");
    writeMetrics(
        output, DATASET_B, IdentifiersPipeline.METRICS_FILENAME, "uniqueIdsCountAttempted: 3\n");
    writeMetrics(
        output,
        DATASET_B,
        MetricsReport.ARCHIVE_TO_VERBATIM_FILENAME,
        "archiveToErCount: 3\nextension_dwc_MeasurementOrFact: 4\n");
    writeMetrics(
        output, DATASET_C, IdentifiersPipeline.METRICS_FILENAME, "uniqueIdsCountAttempted: 1\n");

    PipelinesConfig config = new PipelinesConfig();
    config.setOutputPath(output.getAbsolutePath());
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path reportPath = new Path(tmp.getRoot().getAbsolutePath(), "report.csv");

    Map<String, MetricsReport.DatasetMetadata> metadata =
        Map.of(
            DATASET_A, new MetricsReport.DatasetMetadata("Dataset A", "OCCURRENCE", ""),
            DATASET_B,
                new MetricsReport.DatasetMetadata("Plants, \"B\"", "SAMPLING_EVENT", "INVENTORY"));

    int rows = MetricsReport.writeReport(fs, config, reportPath, metadata, null, null, 2, 60);

    assertEquals(3, rows);
    // the .csv extension is replaced and the report gzipped
    List<String> lines = readGzipLines(new File(tmp.getRoot(), "report.csv.gz"));
    List<String> header = List.of(lines.get(0).split(","));
    int multimedia = header.indexOf("extension_rs_Multimedia");
    int identifier = header.indexOf("extension_rs_Identifier");
    int mof = header.indexOf("extension_dwc_MeasurementOrFact");

    // extension columns are sorted and follow the fixed metric columns
    assertEquals(header.size() - 3, mof);
    assertEquals(mof + 1, identifier);
    assertEquals(mof + 2, multimedia);

    // dataset metadata follows the attempt; B's title is quoted as it has a comma and quotes
    assertEquals(
        List.of("datasetUUID", "attempt", "title", "type", "sub_type"), header.subList(0, 5));
    assertTrue(lines.get(1).startsWith(DATASET_A + ",1,Dataset A,OCCURRENCE,,"));
    assertTrue(
        lines.get(2).startsWith(DATASET_B + ",1,\"Plants, \"\"B\"\"\",SAMPLING_EVENT,INVENTORY,"));
    assertTrue(lines.get(3).startsWith(DATASET_C + ",1,,,,"));

    String[] a = lines.get(1).split(",", -1);
    String[] b = lines.get(2).replace("\"Plants, \"\"B\"\"\"", "B").split(",", -1);
    String[] c = lines.get(3).split(",", -1);
    assertEquals(header.size(), a.length);
    assertEquals(header.size(), c.length);

    assertEquals("7", a[multimedia]);
    assertEquals("2", a[identifier]);
    assertEquals("0", a[mof]);

    assertEquals("0", b[multimedia]);
    assertEquals("0", b[identifier]);
    assertEquals("4", b[mof]);

    assertEquals(MetricsReport.NOT_AVAILABLE, c[multimedia]);
    assertEquals(MetricsReport.NOT_AVAILABLE, c[mof]);
  }

  @Test
  public void existingReportIsRenamedWithTimestamp() throws Exception {
    File output = tmp.newFolder("output");
    writeMetrics(
        output, DATASET_A, IdentifiersPipeline.METRICS_FILENAME, "uniqueIdsCountAttempted: 5\n");

    PipelinesConfig config = new PipelinesConfig();
    config.setOutputPath(output.getAbsolutePath());
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File reportDir = tmp.newFolder("reports");
    File report = new File(reportDir, "report.csv.gz");
    Files.write(report.toPath(), "old report".getBytes(StandardCharsets.UTF_8));
    long modified = report.lastModified();

    MetricsReport.writeReport(
        fs, config, new Path(report.getAbsolutePath()), null, null, null, 1, 60);

    File backup =
        new File(
            reportDir,
            "report-"
                + MetricsReport.BACKUP_SUFFIX_FORMAT.format(
                    java.time.Instant.ofEpochMilli(modified))
                + ".csv.gz");
    assertEquals("old report", Files.readString(backup.toPath()));
    assertEquals(2, readGzipLines(report).size());
  }

  @Test
  public void indexListsLatestAndPreviousRuns() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File reportDir = tmp.newFolder("reports");
    Path report = file(reportDir, "report.csv.gz", 1_000_000L);
    File index = new File(reportDir, MetricsReport.INDEX_FILENAME);

    // first run: nothing to back up
    MetricsReport.updateIndex(fs, report, null, 2);
    assertIndex(index, "report.csv.gz", ts(1_000_000L), List.of());

    // each later run puts the renamed report at the front, keeping at most 2
    MetricsReport.updateIndex(fs, report, file(reportDir, "report-1.csv.gz", 1_000L), 2);
    MetricsReport.updateIndex(fs, report, file(reportDir, "report-2.csv.gz", 2_000L), 2);
    assertIndex(
        index,
        "report.csv.gz",
        ts(1_000_000L),
        List.of(
            new MetricsReport.PreviousRun("report-2.csv.gz", ts(2_000L)),
            new MetricsReport.PreviousRun("report-1.csv.gz", ts(1_000L))));
    MetricsReport.updateIndex(fs, report, file(reportDir, "report-3.csv.gz", 3_000L), 2);
    assertIndex(
        index,
        "report.csv.gz",
        ts(1_000_000L),
        List.of(
            new MetricsReport.PreviousRun("report-3.csv.gz", ts(3_000L)),
            new MetricsReport.PreviousRun("report-2.csv.gz", ts(2_000L))));

    // a differently named report keeps the previous latest, with its timestamp
    Path other = file(reportDir, "other.csv.gz", 2_000_000L);
    MetricsReport.updateIndex(fs, other, null, 2);
    assertIndex(
        index,
        "other.csv.gz",
        ts(2_000_000L),
        List.of(
            new MetricsReport.PreviousRun("report.csv.gz", ts(1_000_000L)),
            new MetricsReport.PreviousRun("report-3.csv.gz", ts(3_000L))));
  }

  @Test
  public void indexWithPlainFilenamesIsUpgraded() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File reportDir = tmp.newFolder("reports");
    Files.writeString(
        new File(reportDir, MetricsReport.INDEX_FILENAME).toPath(),
        "{\"latest\": \"report.csv.gz\", \"previousRuns\": [\"report-1.csv.gz\"]}");
    Path report = file(reportDir, "report.csv.gz", 5_000L);

    MetricsReport.updateIndex(fs, report, file(reportDir, "report-2.csv.gz", 2_000L), 60);

    assertIndex(
        new File(reportDir, MetricsReport.INDEX_FILENAME),
        "report.csv.gz",
        ts(5_000L),
        List.of(
            new MetricsReport.PreviousRun("report-2.csv.gz", ts(2_000L)),
            new MetricsReport.PreviousRun("report-1.csv.gz", null)));
  }

  @Test
  public void writeReportUpdatesIndex() throws Exception {
    File output = tmp.newFolder("output");
    writeMetrics(
        output, DATASET_A, IdentifiersPipeline.METRICS_FILENAME, "uniqueIdsCountAttempted: 5\n");
    PipelinesConfig config = new PipelinesConfig();
    config.setOutputPath(output.getAbsolutePath());
    FileSystem fs = FileSystem.getLocal(new Configuration());
    File reportDir = tmp.newFolder("reports");
    File report = new File(reportDir, "report.csv.gz");
    Path reportPath = new Path(report.getAbsolutePath());

    MetricsReport.writeReport(fs, config, reportPath, null, null, null, 1, 60);
    long modified = report.lastModified();
    MetricsReport.writeReport(fs, config, reportPath, null, null, null, 1, 60);

    String backup =
        "report-"
            + MetricsReport.BACKUP_SUFFIX_FORMAT.format(java.time.Instant.ofEpochMilli(modified))
            + ".csv.gz";
    assertIndex(
        new File(reportDir, MetricsReport.INDEX_FILENAME),
        "report.csv.gz",
        ts(report.lastModified()),
        List.of(new MetricsReport.PreviousRun(backup, ts(modified))));
    assertTrue(new File(reportDir, backup).exists());
  }

  /** Creates a file with the given modification time. */
  private static Path file(File dir, String name, long modified) throws IOException {
    File file = new File(dir, name);
    Files.writeString(file.toPath(), name);
    assertTrue(file.setLastModified(modified));
    return new Path(file.getAbsolutePath());
  }

  private static String ts(long millis) {
    return MetricsReport.INDEX_TIMESTAMP_FORMAT.format(java.time.Instant.ofEpochMilli(millis));
  }

  private static void assertIndex(
      File index,
      String latest,
      String latestTimestamp,
      List<MetricsReport.PreviousRun> previousRuns)
      throws IOException {
    com.fasterxml.jackson.databind.JsonNode json =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(index);
    assertEquals(latest, json.path("latest").asText());
    assertEquals(latestTimestamp, json.path("latestTimestamp").asText());
    List<MetricsReport.PreviousRun> runs = new java.util.ArrayList<>();
    json.path("previousRuns")
        .forEach(
            run ->
                runs.add(
                    new MetricsReport.PreviousRun(
                        run.path("file").asText(),
                        run.path("timestamp").isNull() ? null : run.path("timestamp").asText())));
    assertEquals(previousRuns, runs);
  }

  @Test
  public void reportFilenameAlwaysEndsWithCsvGz() {
    assertEquals("report.csv.gz", MetricsReport.reportFilename("report"));
    assertEquals("report.csv.gz", MetricsReport.reportFilename("report.csv"));
    assertEquals("report.csv.gz", MetricsReport.reportFilename("report.txt"));
    assertEquals("report.csv.gz", MetricsReport.reportFilename("report.csv.gz"));
    assertEquals("report.v2.csv.gz", MetricsReport.reportFilename("report.v2.json"));
  }

  @Test
  public void parsesDatasetExport() throws Exception {
    String header =
        "dataset_key\ttitle\tdoi\tlicense\ttype\tsub_type\thosting_organization_key\t"
            + "occurrence_records_count";
    Map<String, MetricsReport.DatasetMetadata> metadata =
        MetricsReport.parseDatasetExport(
            java.util.stream.Stream.of(
                header,
                DATASET_A + "\tPlain title\t10.1/x\tCC0_1_0\tCHECKLIST\tTAXONOMIC_AUTHORITY\tk\t0",
                DATASET_B
                    + "\t\"Park \"\"Belovezhskaya\"\" (Belarus)\"\t\tCC_BY_4_0\tOCCURRENCE\t\tk\t5",
                "too\tfew\tfields"));

    assertEquals(2, metadata.size());
    assertEquals(
        new MetricsReport.DatasetMetadata("Plain title", "CHECKLIST", "TAXONOMIC_AUTHORITY"),
        metadata.get(DATASET_A));
    assertEquals(
        new MetricsReport.DatasetMetadata("Park \"Belovezhskaya\" (Belarus)", "OCCURRENCE", ""),
        metadata.get(DATASET_B));
  }

  private static List<String> readGzipLines(File file) throws IOException {
    try (InputStream in = new GZIPInputStream(new FileInputStream(file))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
    }
  }

  private static void writeMetrics(File output, String datasetId, String filename, String yaml)
      throws Exception {
    File attemptDir = new File(output, datasetId + "/1");
    attemptDir.mkdirs();
    Files.write(new File(attemptDir, filename).toPath(), yaml.getBytes(StandardCharsets.UTF_8));
  }
}
