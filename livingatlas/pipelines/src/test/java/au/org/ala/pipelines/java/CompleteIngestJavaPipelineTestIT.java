package au.org.ala.pipelines.java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import au.org.ala.pipelines.beam.*;
import au.org.ala.pipelines.options.*;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Complete pipeline tests that use the java variant of the pipeline where possible. Currently this
 * is for Interpretation and SOLR indexing only.
 *
 * <p>This needs to be ran with -Xmx128m
 */
public class CompleteIngestJavaPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "complete_java_pipeline";

  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-pipeline-java"));

    // clear SOLR index
    SolrUtils.setupIndex(INDEX_NAME);

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/complete-pipeline-java/dr893");

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

    // 2. includes samples
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount(INDEX_NAME, "cl620:*"));
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount(INDEX_NAME, "cl927:*"));

    assertEquals(
        Long.valueOf(5),
        SolrUtils.getRecordCount(INDEX_NAME, "dynamicProperties_nonDwcFieldSalinity:*"));

    // 3. has a sensitive record
    assertEquals(Long.valueOf(1), SolrUtils.getRecordCount(INDEX_NAME, "sensitive:generalised"));
    SolrDocument sensitive = SolrUtils.getRecords(INDEX_NAME, "sensitive:generalised").get(0);
    assertEquals(-35.3, (double) sensitive.get("decimalLatitude"), 0.00001);
    assertEquals("-35.260319", sensitive.get("sensitive_decimalLatitude"));

    // 4. check content of record
    CompleteIngestPipelineTestIT.checkSingleRecordContent(INDEX_NAME);
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
              "--properties=" + itUtils.getPropertiesFilePath(),
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
              "--properties=" + itUtils.getPropertiesFilePath(),
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
              "--properties=" + itUtils.getPropertiesFilePath(),
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
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--includeImages=false",
              "--includeSensitiveDataChecks=true"
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
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler lc = new LayerCrawler();
    lc.run(samplingOptions);

    // index into SOLR
    SolrPipelineOptions solrOptions2 =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline-java/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + String.join(",", SolrUtils.getZkHosts()),
              "--solrCollection=" + INDEX_NAME,
              "--includeSampling=true",
              "--includeImages=false",
              "--numOfPartitions=10"
            });
    IndexRecordToSolrPipeline.run(solrOptions2);
  }
}
