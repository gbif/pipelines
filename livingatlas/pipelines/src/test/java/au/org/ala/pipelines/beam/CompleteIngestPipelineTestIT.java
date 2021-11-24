package au.org.ala.pipelines.beam;

import static org.junit.Assert.*;

import au.org.ala.pipelines.options.*;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
@Ignore("Jenkins can't get the response and fails cause of timeout")
public class CompleteIngestPipelineTestIT {

  MockWebServer server;

  @Before
  public void setup() throws Exception {
    server = TestUtils.createMockCollectory();
    server.start(TestUtils.getCollectoryPort());
  }

  @After
  public void teardown() throws Exception {
    server.shutdown();
  }

  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-pipeline"));

    // clear SOLR index
    SolrUtils.setupIndex();

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/complete-pipeline/dr893");

    // reload
    SolrUtils.reloadSolrIndex();

    // validate SOLR index
    assertEquals(Long.valueOf(6), SolrUtils.getRecordCount("*:*"));

    // 1. includes UUIDs
    String documentId = (String) SolrUtils.getRecords("*:*").get(0).get("id");
    assertNotNull(documentId);
    UUID uuid = null;
    try {
      uuid = UUID.fromString(documentId);
      // do something
    } catch (IllegalArgumentException exception) {
      // handle the case where string is not valid UUID
    }

    assertNotNull(uuid);

    // 2. includes samples
    assertEquals(Long.valueOf(6), SolrUtils.getRecordCount("cl620:*"));
    assertEquals(Long.valueOf(6), SolrUtils.getRecordCount("cl927:*"));

    // dynamic properties indexing
    assertEquals(
        Long.valueOf(5), SolrUtils.getRecordCount("dynamicProperties_nonDwcFieldSalinity:*"));

    // 3. has a sensitive record
    assertEquals(Long.valueOf(1), SolrUtils.getRecordCount("sensitive:generalised"));
    SolrDocument sensitive = SolrUtils.getRecords("sensitive:generalised").get(0);
    assertEquals(-35.3, (double) sensitive.get("decimalLatitude"), 0.00001);
    assertEquals("-35.260319", sensitive.get("sensitive_decimalLatitude"));

    // 4. check content of record
    checkSingleRecordContent();
  }

  public static void checkSingleRecordContent() throws Exception {
    Optional<SolrDocument> record = SolrUtils.getRecord("occurrenceID:not-an-uuid-5");

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
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    // solr
    IndexingPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeSensitiveData=true",
              "--includeImages=false"
            });

    // check ready for index - should be true as includeSampling=true and sampling now generated
    assertTrue(ValidationUtils.checkReadyForIndexing(solrOptions).getValid());

    IndexRecordPipeline.run(solrOptions);

    // export lat lngs
    SamplingPipelineOptions samplingOptions =
        PipelinesOptionsFactory.create(
            SamplingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler.init(samplingOptions);
    LayerCrawler.run(samplingOptions);

    SolrPipelineOptions solrOptions2 =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + String.join(",", SolrUtils.getZkHosts()),
              "--solrCollection=" + SolrUtils.BIOCACHE_TEST_SOLR_COLLECTION,
              "--includeSampling=true",
              "--includeSensitiveData=true",
              "--includeImages=false"
            });
    IndexRecordToSolrPipeline.run(solrOptions2);
  }
}
