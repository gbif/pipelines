package au.org.ala.pipelines.java;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

import au.org.ala.pipelines.options.*;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.SolrUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Sandbox pipeline tests that use the java variant of the pipeline where possible.
 *
 * <p>This needs to be ran with -Xmx128m
 */
public class SandboxJavaPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "sandbox_java_pipeline";

  // Safety net to prevent indefinite hangs in CI
  @Rule public final Timeout globalTimeout = new Timeout(10, MINUTES);

  /** Tests for SOLR index creation. */
  @Test
  public void testSandboxPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/sandbox-pipeline-java"));

    // clear SOLR index
    SolrUtils.setupIndex(INDEX_NAME);

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr-test", absolutePath + "/sandbox-pipeline-java/dr-test");

    // reload
    SolrUtils.reloadSolrIndex(INDEX_NAME);

    // validate SOLR index
    assertEquals(Long.valueOf(6), SolrUtils.getRecordCount(INDEX_NAME, "*:*"));

    // 1. includes UUIDs
    String documentId = (String) SolrUtils.getRecords(INDEX_NAME, "*:*").get(0).get("id");
    assertNotNull(documentId);
    UUID uuid = null;
    try {
      uuid = UUID.fromString(documentId);
      // do something
    } catch (IllegalArgumentException exception) {
      // handle the case where string is not valid UUID
    }

    assertNotNull(uuid);

    assertEquals(
        Long.valueOf(5),
        SolrUtils.getRecordCount(INDEX_NAME, "dynamicProperties_nonDwcFieldSalinity:*"));

    // 2. check content of record
    checkSingleRecordContent(INDEX_NAME);
  }

  public static void checkSingleRecordContent(String currentIndexName) throws Exception {
    Optional<SolrDocument> record =
        SolrUtils.getRecord(currentIndexName, "occurrenceID:not-an-uuid-5");

    assertTrue(record.isPresent());
    assertEquals("not-an-uuid-5", record.get().get("occurrenceID"));

    assertEquals("Scioglyptis chionomera", record.get().get("scientificName"));
    assertEquals("Scioglyptis chionomera", record.get().get("raw_scientificName"));

    assertEquals("Animalia", record.get().get("raw_kingdom"));
    assertEquals("Animalia", record.get().get("kingdom"));

    assertEquals("Arthropoda", record.get().get("phylum"));
    assertEquals("Arthropoda", record.get().get("raw_phylum"));
    assertEquals("Insecta", record.get().get("class"));
    assertEquals("Insecta", record.get().get("raw_class"));
    assertEquals("Lepidoptera", record.get().get("order"));
    assertEquals("Lepidoptera", record.get().get("raw_order"));
    assertEquals("Geometridae", record.get().get("family"));
    assertEquals("Geometridae", record.get().get("raw_family"));
    assertEquals("Scioglyptis", record.get().get("genus"));
    assertEquals("Scioglyptis", record.get().get("raw_genus"));

    assertEquals("species", record.get().get("raw_taxonRank"));
    assertEquals("species", record.get().get("taxonRank"));
    assertEquals(7000, record.get().get("taxonRankID"));

    Date eventDate = (Date) record.get().get("eventDate");
    assertNotNull(eventDate);

    assertEquals("2016-11-26", new SimpleDateFormat("yyyy-MM-dd").format(eventDate));
    assertEquals("26/11/16", record.get().get("raw_eventDate"));
    assertEquals(26, record.get().get("day"));
    assertEquals("26", record.get().get("raw_day"));
    assertEquals(11, record.get().get("month"));
    assertEquals("11", record.get().get("raw_month"));
    assertEquals(2016, record.get().get("year"));
    assertEquals("2016", record.get().get("raw_year"));

    assertEquals("Victoria", record.get().get("raw_stateProvince"));
    assertEquals("Australia", record.get().get("raw_country"));
    assertEquals("Victoria", record.get().get("stateProvince"));
    assertEquals("Australia", record.get().get("country"));
    assertNull(record.get().get("raw_countryCode"));
    assertEquals("AU", record.get().get("countryCode"));

    assertEquals("-37.9909881", record.get().get("raw_decimalLatitude"));
    assertEquals("145.1256931", record.get().get("raw_decimalLongitude"));
    assertEquals(-37.990988, record.get().get("decimalLatitude"));
    assertEquals(145.125693, record.get().get("decimalLongitude"));
    assertEquals("EPSG:4326", record.get().get("raw_" + DwcTerm.geodeticDatum.simpleName()));

    // 'other' is not in the degreeOfEstablishment vocab so will present as null when processed
    assertNull(record.get().get("degreeOfEstablishment"));
    assertEquals("other", record.get().get("raw_degreeOfEstablishment"));
    assertEquals("native", record.get().get("raw_establishmentMeans"));
    assertEquals("native", record.get().get("establishmentMeans"));

    // recordByID and identifiedByID
    assertEquals("id3", record.get().get("raw_recordedByID"));
    assertEquals("id3", ((List) record.get().get("recordedByID")).get(0));
    assertEquals("id1|id2", record.get().get("raw_identifiedByID"));
    assertEquals("id1", ((List) record.get().get("identifiedByID")).get(0));
    assertEquals("id2", ((List) record.get().get("identifiedByID")).get(1));

    // raw state and raw country conservation
    assertEquals("Extinct", record.get().get("raw_stateConservation"));
    assertEquals("Vulnerable", record.get().get("raw_countryConservation"));

    // organismQuantity
    assertEquals("e", record.get().get("organismQuantity"));
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {
    DwcaToSolrPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaToSolrPipelineOptions.class,
            new String[] {
              "--attempt=1",
              "--datasetId=" + datasetID,
              "--targetPath=/tmp/la-pipelines-test/sandbox-pipeline-java",
              "--inputPath=" + inputPath,
              "--solrHost=http://" + SolrUtils.getHttpHost() + "/solr",
              "--solrCollection=" + INDEX_NAME,
              "--includeSampling=false",
              "--properties=" + itUtils.getSandboxPropertiesFilePath()
            });
    DwcaToSolrPipeline.run(dwcaOptions);
  }
}
