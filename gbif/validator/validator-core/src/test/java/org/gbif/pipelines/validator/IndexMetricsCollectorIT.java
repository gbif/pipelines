package org.gbif.pipelines.validator;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.pojo.FileNameTerm;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.IssueSample;
import org.gbif.validator.api.Metrics.TermInfo;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class IndexMetricsCollectorIT {

  // files for testing
  private static final Path MAPPINGS_PATH = Paths.get("mappings/verbatim-mapping.json");
  private static final String IDX_NAME = "validator";

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void collecorTest() {
    // State

    String datasetKey = "675a1bfd-9bcc-46ea-a417-1f68f23a10f6";

    String documentOne =
        "{\"datasetKey\":\"675a1bfd-9bcc-46ea-a417-1f68f23a10f6\",\"id\":\"bla\",\"maximumElevationInMeters\":2.2,\"issues\":"
            + "[\"GEODETIC_DATUM_ASSUMED_WGS84\",\"ELEVATION_UNLIKELY\"],\"verbatim\":{\"core\":"
            + "{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"1150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"251\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"1.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.83\"}]}}}";

    String documentTwo =
        "{\"datasetKey\":\"675a1bfd-9bcc-46ea-a417-1f68f23a10f6\",\"id\":\"bla1\",\"maximumElevationInMeters\":3.2,\"issues\":"
            + "[\"RANDOM_ISSUE\"],\"verbatim\":{\"core\":{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"2150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"351\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"2.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"6.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"6.83\"}]}}}";

    String documentThree =
        "{\"datasetKey\":\"675a1bfd-9bcc-46ea-a417-1f68f23a10f6\",\"id\":\"bla2\",\"maximumElevationInMeters\":3.2,"
            + "\"issues\":[\"RANDOM_ISSUE\"],\"verbatim\":{\"core\":{}}}";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(IDX_NAME)
            .settingsType(INDEXING)
            .pathMappings(MAPPINGS_PATH)
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), IDX_NAME, 1L, documentOne);
    EsService.indexDocument(ES_SERVER.getEsClient(), IDX_NAME, 2L, documentTwo);
    EsService.indexDocument(ES_SERVER.getEsClient(), IDX_NAME, 3L, documentThree);
    EsService.refreshIndex(ES_SERVER.getEsClient(), IDX_NAME);

    // When
    Map<FileNameTerm, Set<Term>> coreTerms =
        Collections.singletonMap(
            FileNameTerm.create("file.txt", DwcTerm.Occurrence.qualifiedName()),
            new HashSet<>(
                Arrays.asList(
                    DwcTerm.maximumElevationInMeters,
                    DwcTerm.organismID,
                    DwcTerm.occurrenceID,
                    DwcTerm.bed)));

    Map<FileNameTerm, Set<Term>> extTerms =
        Collections.singletonMap(
            FileNameTerm.create("ext.txt", Extension.MEASUREMENT_OR_FACT.getRowType()),
            new HashSet<>(Arrays.asList(DwcTerm.measurementValue, DwcTerm.measurementType)));

    Metrics result =
        IndexMetricsCollector.builder()
            .coreTerms(coreTerms)
            .extensionsTerms(extTerms)
            .key(UUID.fromString(datasetKey))
            .index(IDX_NAME)
            .corePrefix("verbatim.core")
            .extensionsPrefix("verbatim.extensions")
            .esHost(ES_SERVER.getEsConfig().getRawHosts())
            .build()
            .collect();

    // Should

    // Core
    assertEquals(2, result.getFileInfos().size());

    Optional<FileInfo> coreOpt = getFileInfo(result, DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    List<TermInfo> resCoreTerms = core.getTerms();

    assertEquals(Long.valueOf(3L), core.getIndexedCount());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.maximumElevationInMeters.qualifiedName())
            .rawIndexed(2L)
            .interpretedIndexed(3L)
            .build());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.organismID.qualifiedName())
            .rawIndexed(2L)
            .interpretedIndexed(0L)
            .build());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.occurrenceID.qualifiedName())
            .rawIndexed(0L)
            .interpretedIndexed(0L)
            .build());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.bed.qualifiedName())
            .rawIndexed(2L)
            .interpretedIndexed(null)
            .build());

    Optional<TermInfo> anyTerm =
        resCoreTerms.stream()
            .filter(x -> x.getTerm().equalsIgnoreCase(DwcTerm.county.qualifiedName()))
            .findAny();
    assertFalse(anyTerm.isPresent());

    // OccurrenceIssues
    List<IssueInfo> issues = core.getIssues();
    assertEquals(3, issues.size());
    assertIssueInfo(
        issues,
        IssueInfo.builder()
            .issue("RANDOM_ISSUE")
            .count(2L)
            .samples(
                Arrays.asList(
                    IssueSample.builder().recordId("bla1").build(),
                    IssueSample.builder().recordId("bla2").build()))
            .build());

    assertIssueInfo(
        issues,
        IssueInfo.builder()
            .issue(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name())
            .count(1L)
            .samples(Collections.singletonList(IssueSample.builder().recordId("bla").build()))
            .build());

    assertIssueInfo(
        issues,
        IssueInfo.builder()
            .issue(OccurrenceIssue.ELEVATION_UNLIKELY.name())
            .count(1L)
            .samples(
                Collections.singletonList(
                    IssueSample.builder()
                        .relatedData(
                            Collections.singletonMap(
                                DwcTerm.maximumElevationInMeters.toString(), "1150"))
                        .recordId("bla")
                        .build()))
            .build());

    // Extensions
    Optional<FileInfo> extensionOpt =
        getFileInfo(result, Extension.MEASUREMENT_OR_FACT.getRowType());
    assertTrue(extensionOpt.isPresent());
    FileInfo extension = extensionOpt.get();

    Optional<TermInfo> measurementValueOpt = getTermInfo(extension, DwcTerm.measurementValue);
    assertTrue(measurementValueOpt.isPresent());
    assertEquals(Long.valueOf(6L), measurementValueOpt.get().getRawIndexed());

    Optional<TermInfo> measurementTypeOpt = getTermInfo(extension, DwcTerm.measurementType);
    assertTrue(measurementTypeOpt.isPresent());
    assertEquals(Long.valueOf(0L), measurementTypeOpt.get().getRawIndexed());

    Optional<TermInfo> countyOpt = getTermInfo(extension, DwcTerm.county);
    assertFalse(countyOpt.isPresent());
  }

  private Optional<FileInfo> getFileInfo(Metrics metrics, String term) {
    return metrics.getFileInfos().stream().filter(x -> x.getRowType().equals(term)).findAny();
  }

  private Optional<FileInfo> getFileInfo(Metrics metrics, Term term) {
    return getFileInfo(metrics, term.qualifiedName());
  }

  private Optional<TermInfo> getTermInfo(FileInfo fileInfo, Term term) {
    return fileInfo.getTerms().stream()
        .filter(x -> x.getTerm().equals(term.qualifiedName()))
        .findAny();
  }

  private void assertTermInfo(List<TermInfo> list, TermInfo expected) {
    Optional<TermInfo> anyTerm =
        list.stream().filter(x -> x.getTerm().equalsIgnoreCase(expected.getTerm())).findAny();
    assertTrue(anyTerm.isPresent());
    TermInfo info = anyTerm.get();
    assertEquals(expected.getRawIndexed(), info.getRawIndexed());
    assertEquals(expected.getInterpretedIndexed(), info.getInterpretedIndexed());
  }

  private void assertIssueInfo(List<IssueInfo> list, IssueInfo expected) {
    Optional<IssueInfo> anyTerm =
        list.stream().filter(x -> x.getIssue().equalsIgnoreCase(expected.getIssue())).findAny();
    assertTrue(anyTerm.isPresent());
    IssueInfo info = anyTerm.get();
    assertEquals(expected.getCount(), info.getCount());
    assertEquals(expected.getSamples().size(), info.getSamples().size());

    Map<String, Map<String, String>> infoAsMap =
        info.getSamples().stream()
            .collect(Collectors.toMap(IssueSample::getRecordId, IssueSample::getRelatedData));

    for (IssueSample is : expected.getSamples()) {
      assertTrue(infoAsMap.containsKey(is.getRecordId()));
      if (is.getRelatedData() == null) {
        assertNull(infoAsMap.get(is.getRecordId()).get(is.getRecordId()));
      } else {
        for (Entry<String, String> entry : is.getRelatedData().entrySet()) {
          assertEquals(entry.getValue(), infoAsMap.get(is.getRecordId()).get(entry.getKey()));
        }
      }
    }
  }
}
