package au.org.ala.pipelines.beam;

import static org.junit.Assert.*;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.Map;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.ingest.java.io.AvroReader;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class SensitiveDataPipelineTestIT {
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

  /**
   * Tests for SOLR index creation.
   *
   * @throws Exception
   */
  @Test
  public void testSensitivePipeline() throws Exception {

    File shapefiles = new File("/tmp/pipelines-shp");
    assertTrue(
        "The shapefiles "
            + shapefiles
            + " should be loaded. If you are running this test standalone use mvn pre-integration-test",
        shapefiles.exists());
    // clear up previous test runs
    File pipeline = new File("/tmp/la-pipelines-test/sensitive-pipeline");
    FileUtils.deleteQuietly(pipeline);

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/sensitive-data/dr893");

    File dr = new File(pipeline, "dr893/1");
    File interpreted = new File(dr, "interpreted");
    assertTrue(interpreted.exists());
    File generalised = new File(dr, "generalised");
    assertTrue(generalised.exists());
    File ala_sensitive_data = new File(generalised, "ala_sensitive_data");
    assertTrue(ala_sensitive_data.exists());
    File location = new File(generalised, "location");
    assertTrue(location.exists());
    File verbatim = new File(generalised, "verbatim");
    assertTrue(verbatim.exists());

    // Check correctly stated sensitivity
    Map<String, ALASensitivityRecord> sds =
        AvroReader.readRecords(
            null, null, ALASensitivityRecord.class, ala_sensitive_data.getPath() + "/*.avro");
    ALASensitivityRecord sds1 = sds.get("not-an-uuid-1");
    assertNotNull(sds1);
    assertTrue(sds1.getSensitive());
    assertEquals(
        "Location in New South Wales, Australia generalised to 0.1 degrees. \nSensitive in NSW, Name: New South Wales, Zone: STATE [NSW Category 2 Conservation Protected, NSW OEH]",
        sds1.getDataGeneralizations());
    assertEquals("149.4", sds1.getAltered().get("decimalLongitude"));
    ALASensitivityRecord sds2 = sds.get("not-an-uuid-2");
    assertNotNull(sds2);
    assertFalse(sds2.getSensitive());

    // Check location generalisation
    Map<String, LocationRecord> locs =
        AvroReader.readRecords(null, null, LocationRecord.class, location.getPath() + "/*.avro");
    LocationRecord loc1 = locs.get("not-an-uuid-1");
    assertNotNull(loc1);
    assertEquals(149.4, loc1.getDecimalLongitude(), 0.0001);
    assertEquals(-35.3, loc1.getDecimalLatitude(), 0.0001);
    LocationRecord loc2 = locs.get("not-an-uuid-2");
    assertNotNull(loc2);
    assertEquals(144.322971, loc2.getDecimalLongitude(), 0.000001);
    assertEquals(-38.211987, loc2.getDecimalLatitude(), 0.000001);

    // Check verbatim records
    Map<String, ExtendedRecord> verbs =
        AvroReader.readRecords(null, null, ExtendedRecord.class, verbatim.getPath() + "/*.avro");
    ExtendedRecord vb1 = verbs.get("not-an-uuid-1");
    assertNotNull(vb1);
    assertEquals("149.4", vb1.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals("-35.3", vb1.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    ExtendedRecord vb2 = verbs.get("not-an-uuid-2");
    assertNotNull(vb2);
    assertEquals("144.322971", vb2.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals("-38.211987", vb2.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
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
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
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
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline/dr893/1/verbatim.avro",
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
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    // sensitive data
    InterpretationPipelineOptions sensitiveOptions =
        PipelinesOptionsFactory.create(
            ALASolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + SolrUtils.getZkHost(),
              "--solrCollection=" + SolrUtils.BIOCACHE_TEST_SOLR_COLLECTION,
              "--includeSampling=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitiveOptions);
  }
}
