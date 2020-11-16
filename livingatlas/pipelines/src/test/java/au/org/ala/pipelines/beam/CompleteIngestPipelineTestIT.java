package au.org.ala.pipelines.beam;

import static org.junit.Assert.*;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.UUID;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
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

    // 3. has a sensitive record
    assertEquals(Long.valueOf(1), SolrUtils.getRecordCount("sensitive:true"));
    SolrDocument sensitive = SolrUtils.getRecords("sensitive:true").get(0);
    assertEquals(-35.3, (double) sensitive.get("decimalLatitude"), 0.00001);
    assertEquals("-35.260319", sensitive.get("original_decimalLatitude"));
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
    ALASolrPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            ALASolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + SolrUtils.getZkHost(),
              "--solrCollection=" + SolrUtils.BIOCACHE_TEST_SOLR_COLLECTION,
              "--includeSampling=true",
              "--includeImages=false"
            });

    // check ready for index - should be false as includeSampling=true and sampling not generated
    // yet
    assertFalse(ValidationUtils.checkReadyForIndexing(solrOptions).getValid());

    // export lat lngs
    AllDatasetsPipelinesOptions latLngOptions =
        PipelinesOptionsFactory.create(
            AllDatasetsPipelinesOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });
    ALAInterpretedToLatLongCSVPipeline.run(latLngOptions);

    // sample
    LayerCrawler.init(
        (new CombinedYamlConfiguration(
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--config=" + TestUtils.getPipelinesConfigFile()
            })));
    LayerCrawler.run(latLngOptions);

    // sample -> avro
    AllDatasetsPipelinesOptions samplingAvroOptions =
        PipelinesOptionsFactory.create(
            AllDatasetsPipelinesOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--metaFileName=" + ValidationUtils.SAMPLING_METRICS,
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });
    ALASamplingToAvroPipeline.run(samplingAvroOptions);

    // check ready for index - should be true as includeSampling=true and sampling now generated
    assertTrue(ValidationUtils.checkReadyForIndexing(solrOptions).getValid());

    ALAInterpretedToSolrIndexPipeline.run(solrOptions);
  }
}
