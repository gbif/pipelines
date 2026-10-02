package org.gbif.pipelines.tasks.validators.metrics.collector;

import static org.gbif.validator.api.EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the Spark-based overlay logic that replaced the ES-based {@code IndexMetricsCollector}.
 * {@code collect-metrics.json} and the interpretation meta file are read via the local filesystem
 * (no HDFS/ES infra needed) since {@code hdfsSiteConfig}/{@code coreSiteConfig} are left empty,
 * which makes {@link org.gbif.pipelines.core.factory.FileSystemFactory} fall back to the local
 * filesystem.
 */
public class SparkMetricsReaderTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void readSparkMetricsTest() throws IOException {
    // State
    UUID key = UUID.randomUUID();
    int attempt = 1;
    writeAttemptFile(
        key,
        attempt,
        "collect-metrics.json",
        "{\"files\":[{\"rowType\":\""
            + DwcTerm.Occurrence.qualifiedName()
            + "\",\"indexedCount\":42,\"terms\":[{\"term\":\""
            + DwcTerm.scientificName.qualifiedName()
            + "\",\"interpretedIndexed\":40}]}]}");

    MetricsCollectorConfiguration config = createConfig();
    PipelinesIndexedMessage message = createMessage(key, attempt);

    // When
    Metrics result = SparkMetricsReader.readSparkMetrics(config, message);

    // Should
    assertEquals(1, result.getFileInfos().size());
    FileInfo fileInfo = result.getFileInfos().get(0);
    assertEquals(DwcTerm.Occurrence.qualifiedName(), fileInfo.getRowType());
    assertEquals(Long.valueOf(42L), fileInfo.getIndexedCount());
    assertEquals(Long.valueOf(40L), fileInfo.getTerms().get(0).getInterpretedIndexed());
  }

  @Test
  public void readSparkMetricsMissingFileTest() {
    // State
    MetricsCollectorConfiguration config = createConfig();
    PipelinesIndexedMessage message = createMessage(UUID.randomUUID(), 1);

    // When/Should
    assertThrows(IOException.class, () -> SparkMetricsReader.readSparkMetrics(config, message));
  }

  @Test
  public void applySparkMetricsOverlayTest() {
    // State - raw file infos, as produced by reading the source archive
    TermInfo scientificName =
        TermInfo.builder().term(DwcTerm.scientificName.qualifiedName()).rawIndexed(5L).build();
    TermInfo county =
        TermInfo.builder().term(DwcTerm.county.qualifiedName()).rawIndexed(3L).build();
    FileInfo occurrence =
        FileInfo.builder()
            .fileName("occurrence.txt")
            .fileType(DwcFileType.CORE)
            .rowType(DwcTerm.Occurrence.qualifiedName())
            .terms(new ArrayList<>(Arrays.asList(scientificName, county)))
            .build();
    List<FileInfo> fileInfos = new ArrayList<>(Collections.singletonList(occurrence));

    // Spark-computed metrics
    IssueInfo issue = IssueInfo.builder().issue("RANDOM_ISSUE").count(2L).build();
    TermInfo sparkScientificName =
        TermInfo.builder()
            .term(DwcTerm.scientificName.qualifiedName())
            .interpretedIndexed(5L)
            .build();
    FileInfo sparkOccurrence =
        FileInfo.builder()
            .rowType(DwcTerm.Occurrence.qualifiedName())
            .indexedCount(10L)
            .issues(Collections.singletonList(issue))
            .terms(Collections.singletonList(sparkScientificName))
            .build();
    Metrics sparkMetrics =
        Metrics.builder().fileInfos(Collections.singletonList(sparkOccurrence)).build();

    // When
    SparkMetricsReader.applySparkMetrics(fileInfos, sparkMetrics);

    // Should
    FileInfo result = fileInfos.get(0);
    assertEquals(Long.valueOf(10L), result.getIndexedCount());
    assertEquals(1, result.getIssues().size());
    assertEquals("RANDOM_ISSUE", result.getIssues().get(0).getIssue());

    TermInfo scientificNameResult =
        result.getTerms().stream()
            .filter(t -> t.getTerm().equals(DwcTerm.scientificName.qualifiedName()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    assertEquals(Long.valueOf(5L), scientificNameResult.getInterpretedIndexed());

    // Terms absent from the Spark output are left untouched
    TermInfo countyResult =
        result.getTerms().stream()
            .filter(t -> t.getTerm().equals(DwcTerm.county.qualifiedName()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    assertNull(countyResult.getInterpretedIndexed());
  }

  @Test
  public void updateIssuesFromMetaInfosAddsIssueTest() throws IOException {
    // State
    UUID key = UUID.randomUUID();
    int attempt = 1;
    writeAttemptFile(key, attempt, "verbatim-to-occurrence.yml", "duplicatedIdsCountAttempted: 2");

    MetricsCollectorConfiguration config = createConfig();
    PipelinesIndexedMessage message = createMessage(key, attempt);

    FileInfo coreFileInfo = FileInfo.builder().fileType(DwcFileType.CORE).build();
    Validation validation =
        Validation.builder()
            .metrics(Metrics.builder().fileInfos(Collections.singletonList(coreFileInfo)).build())
            .build();

    // When
    SparkMetricsReader.updateIssuesFromMetaInfos(config, message, validation);

    // Should
    List<IssueInfo> issues = validation.getMetrics().getFileInfos().get(0).getIssues();
    assertTrue(
        issues.stream()
            .anyMatch(i -> i.getIssue().equals(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED.name())));
  }

  @Test
  public void updateIssuesFromMetaInfosNoDuplicatesTest() throws IOException {
    // State
    UUID key = UUID.randomUUID();
    int attempt = 1;
    writeAttemptFile(key, attempt, "verbatim-to-occurrence.yml", "duplicatedIdsCountAttempted: 0");

    MetricsCollectorConfiguration config = createConfig();
    PipelinesIndexedMessage message = createMessage(key, attempt);

    FileInfo coreFileInfo = FileInfo.builder().fileType(DwcFileType.CORE).build();
    Validation validation =
        Validation.builder()
            .metrics(Metrics.builder().fileInfos(Collections.singletonList(coreFileInfo)).build())
            .build();

    // When
    SparkMetricsReader.updateIssuesFromMetaInfos(config, message, validation);

    // Should
    assertTrue(validation.getMetrics().getFileInfos().get(0).getIssues().isEmpty());
  }

  private void writeAttemptFile(UUID key, int attempt, String fileName, String content)
      throws IOException {
    Path attemptDir =
        folder.getRoot().toPath().resolve(key.toString()).resolve(String.valueOf(attempt));
    Files.createDirectories(attemptDir);
    Files.write(attemptDir.resolve(fileName), content.getBytes(StandardCharsets.UTF_8));
  }

  private MetricsCollectorConfiguration createConfig() {
    MetricsCollectorConfiguration config = new MetricsCollectorConfiguration();
    config.stepConfig.repositoryPath = folder.getRoot().getPath();
    config.stepConfig.hdfsSiteConfig = "";
    config.stepConfig.coreSiteConfig = "";
    return config;
  }

  private PipelinesIndexedMessage createMessage(UUID key, int attempt) {
    PipelinesIndexedMessage message = new PipelinesIndexedMessage();
    message.setDatasetUuid(key);
    message.setAttempt(attempt);
    return message;
  }
}
