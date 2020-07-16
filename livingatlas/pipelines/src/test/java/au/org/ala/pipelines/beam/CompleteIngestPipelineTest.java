package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.SolrUtils;
import java.io.File;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class CompleteIngestPipelineTest {

  /**
   * Tests for SOLR index creation.
   *
   * @throws Exception
   */
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
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount("*:*"));

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
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount("cl620:*"));
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount("cl927:*"));
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=dwca-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=interpretation-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
              "--properties=src/test/resources/pipelines.yaml",
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    InterpretationPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=uuid-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
              "--properties=src/test/resources/pipelines.yaml",
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // export lat lngs
    InterpretationPipelineOptions latLngOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=src/test/resources/pipelines.yaml"
            });
    ALAInterpretedToLatLongCSVPipeline.run(latLngOptions);

    // sample
    LayerCrawler.run(latLngOptions);

    // sample -> avro
    ALASamplingToAvroPipeline.run(latLngOptions);

    // solr
    ALASolrPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            ALASolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=uuid-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
              "--properties=src/test/resources/pipelines.yaml",
              "--zkHost=localhost:9983",
              "--solrCollection=" + SolrUtils.BIOCACHE_TEST_SOLR_COLLECTION,
              "--includeSampling=true"
            });

    ALAInterpretedToSolrIndexPipeline.run(solrOptions);
  }
}
