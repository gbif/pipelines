package org.gbif.pipelines.validator;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.validator.metircs.Metrics;
import org.gbif.pipelines.validator.metircs.Metrics.Core;
import org.gbif.pipelines.validator.metircs.MetricsCollector;
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
        "{\"datasetKey\":\"675a1bfd-9bcc-46ea-a417-1f68f23a10f6\",\"issues\":[\"GEODETIC_DATUM_ASSUMED_WGS84\","
            + "\"RANDOM_ISSUE\"],\"verbatim\":{\"core\":{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":"
            + "\"1150\",\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\"},\"extensions\":"
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
                DwcTerm.maximumElevationInMeters, DwcTerm.organismID, DwcTerm.occurrenceID));

    Map<Extension, Set<Term>> extTerms =
        Collections.singletonMap(
            Extension.MEASUREMENT_OR_FACT,
            new HashSet<>(Arrays.asList(DwcTerm.measurementValue, DwcTerm.measurementType)));

    Metrics result =
        MetricsCollector.builder()
            .coreTerms(coreTerms)
            .extensionsTerms(extTerms)
            .datasetKey(datasetKey)
            .index(IDX_NAME)
            .corePrefix("verbatim.core")
            .extensionsPrefix("verbatim.extensions")
            .esHost(ES_SERVER.getEsConfig().getRawHosts())
            .build()
            .collect();

    // Should

    // Metrics
    assertEquals(datasetKey, result.getDatasetKey());
    assertEquals(ValidationStatus.FINISHED, result.getStatus());
    assertNotNull(result.getEndTimestamp());

    // Core
    Core core = result.getResult().getCore();
    Map<String, Long> resCoreTerms = core.getIndexedCoreTerm();
    assertEquals(Long.valueOf(1L), core.getIndexedCount());
    assertEquals(
        Long.valueOf(1L), resCoreTerms.get(DwcTerm.maximumElevationInMeters.qualifiedName()));
    assertEquals(Long.valueOf(1L), resCoreTerms.get(DwcTerm.organismID.qualifiedName()));
    assertEquals(Long.valueOf(0L), resCoreTerms.get(DwcTerm.occurrenceID.qualifiedName()));
    assertNull(resCoreTerms.get(DwcTerm.county.qualifiedName()));

    // OccurrenceIssues
    Map<String, Long> issues = core.getOccurrenceIssuesMap();
    assertEquals(2, issues.size());
    assertEquals(Long.valueOf(1L), issues.get("RANDOM_ISSUE"));
    assertEquals(Long.valueOf(1L), issues.get("GEODETIC_DATUM_ASSUMED_WGS84"));

    // Extensions
    Map<String, Long> resExtTerms =
        result.getResult().getExtensions().get(0).getExtensionsTermsCountMap();
    assertEquals(Long.valueOf(1L), resExtTerms.get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals(Long.valueOf(0L), resExtTerms.get(DwcTerm.measurementType.qualifiedName()));
    assertNull(resExtTerms.get(DwcTerm.county.qualifiedName()));
  }
}
