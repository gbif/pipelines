package au.org.ala.pipelines.java;

import static au.org.ala.pipelines.beam.CompleteIngestPipelineTestIT.checkSingleRecordContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import au.org.ala.pipelines.beam.*;
import au.org.ala.pipelines.options.*;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
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
import org.junit.Ignore;
import org.junit.Test;

/**
 * Complete pipeline tests that use the java variant of the pipeline where possible. Currently this
 * is for Interpretation and SOLR indexing only.
 *
 * <p>This needs to be ran with -Xmx128m
 */
@Ignore("Jenkins can't get the response and fails cause of timeout")
public class CompleteIngestJavaPipelineTestIT {

  MockWebServer server;

  @Before
  public void setup() throws Exception {
    // clear up previous test runs
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
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-pipeline-java"));

    // clear SOLR index
    SolrUtils.setupIndex();

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/complete-pipeline-java/dr893");

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

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    // convert DwCA
    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--appName=DWCA",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // interpret
    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java/dr893/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // validate and create UUIDs
    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // run SDS checks
    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    // index record generation
    IndexingPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline-java/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--includeImages=false",
              "--includeSensitiveData=true"
            });
    IndexRecordPipeline.run(solrOptions);

    // export lat lngs
    SamplingPipelineOptions samplingOptions =
        PipelinesOptionsFactory.create(
            SamplingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline-java/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler.init(samplingOptions);
    LayerCrawler.run(samplingOptions);

    // index into SOLR
    SolrPipelineOptions solrOptions2 =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline-java/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + String.join(",", SolrUtils.getZkHosts()),
              "--solrCollection=" + SolrUtils.BIOCACHE_TEST_SOLR_COLLECTION,
              "--includeSampling=true",
              "--includeImages=false"
            });
    IndexRecordToSolrPipeline.run(solrOptions2);
  }
}
