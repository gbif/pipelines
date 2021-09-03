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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.Core;
import org.gbif.validator.api.Metrics.Core.IssueInfo;
import org.gbif.validator.api.Metrics.Core.TermInfo;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MetricsCollectorIT {

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

    String document =
        "{\"datasetKey\":\"675a1bfd-9bcc-46ea-a417-1f68f23a10f6\",\"maximumElevationInMeters\":2.2,\"issues\":"
            + "[\"GEODETIC_DATUM_ASSUMED_WGS84\",\"RANDOM_ISSUE\"],\"verbatim\":{\"core\":"
            + "{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"1150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"251\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"1.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.83\"}]}}}";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(IDX_NAME)
            .settingsType(INDEXING)
            .pathMappings(MAPPINGS_PATH)
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), IDX_NAME, 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), IDX_NAME);

    // When
    Set<Term> coreTerms =
        new HashSet<>(
            Arrays.asList(
                DwcTerm.maximumElevationInMeters,
                DwcTerm.organismID,
                DwcTerm.occurrenceID,
                DwcTerm.bed));

    Map<Extension, Set<Term>> extTerms =
        Collections.singletonMap(
            Extension.MEASUREMENT_OR_FACT,
            new HashSet<>(Arrays.asList(DwcTerm.measurementValue, DwcTerm.measurementType)));

    Metrics result =
        MetricsCollector.builder()
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
    Core core = result.getCore();
    Set<TermInfo> resCoreTerms = core.getIndexedCoreTerms();

    assertEquals(Long.valueOf(1L), core.getIndexedCount());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.maximumElevationInMeters.qualifiedName())
            .rawIndexed(1L)
            .interpretedIndexed(1L)
            .build());

    assertTermInfo(
        resCoreTerms,
        TermInfo.builder()
            .term(DwcTerm.organismID.qualifiedName())
            .rawIndexed(1L)
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
            .rawIndexed(1L)
            .interpretedIndexed(null)
            .build());

    Optional<TermInfo> anyTerm =
        resCoreTerms.stream()
            .filter(x -> x.getTerm().equalsIgnoreCase(DwcTerm.county.qualifiedName()))
            .findAny();
    assertFalse(anyTerm.isPresent());

    // OccurrenceIssues
    Set<IssueInfo> issues = core.getOccurrenceIssues();
    assertEquals(2, issues.size());
    assertIssueInfo(issues, IssueInfo.builder().issue("RANDOM_ISSUE").count(1L).build());
    assertIssueInfo(
        issues, IssueInfo.builder().issue("GEODETIC_DATUM_ASSUMED_WGS84").count(1L).build());

    // Extensions
    Metrics.Extension extension = result.getExtensions().get(0);

    Map<String, Long> resExtTerms = extension.getExtensionsTermsCounts();
    assertEquals(Long.valueOf(3L), resExtTerms.get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals(Long.valueOf(0L), resExtTerms.get(DwcTerm.measurementType.qualifiedName()));
    assertNull(resExtTerms.get(DwcTerm.county.qualifiedName()));
  }

  private void assertTermInfo(Set<TermInfo> set, TermInfo termInfo) {
    Optional<TermInfo> anyTerm =
        set.stream().filter(x -> x.getTerm().equalsIgnoreCase(termInfo.getTerm())).findAny();
    assertTrue(anyTerm.isPresent());
    TermInfo info = anyTerm.get();
    assertEquals(termInfo.getRawIndexed(), info.getRawIndexed());
    assertEquals(termInfo.getInterpretedIndexed(), info.getInterpretedIndexed());
  }

  private void assertIssueInfo(Set<IssueInfo> set, IssueInfo issueInfo) {
    Optional<IssueInfo> anyTerm =
        set.stream().filter(x -> x.getIssue().equalsIgnoreCase(issueInfo.getIssue())).findAny();
    assertTrue(anyTerm.isPresent());
    IssueInfo info = anyTerm.get();
    assertEquals(issueInfo.getCount(), info.getCount());
  }
}
