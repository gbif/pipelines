package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ALASolrPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.SolrUtils;
import org.codehaus.plexus.util.FileUtils;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index.
 * Includes all current steps in processing.
 */
public class CompleteIngestPipelineTest {

    /**
     * Tests for SOLR index creation.
     *
     * @throws Exception
     */
    @Test
    public void testIngestPipeline() throws Exception {

        //clear up previous test runs
        FileUtils.forceDelete("/tmp/la-pipelines-test/complete-pipeline");

        //clear SOLR index
        SolrUtils.setupIndex();

        String absolutePath = new File("src/test/resources").getAbsolutePath();

        // Step 1: load a dataset and verify all records have a UUID associated
        loadTestDataset("dr893", absolutePath + "/complete-pipeline/dr893");

        //reload
        SolrUtils.reloadSolrIndex();

        //validate SOLR index
        assert SolrUtils.getRecordCount("*:*") == 5;

        //1. includes UUIDs
        String documentId = (String) SolrUtils.getRecords("*:*").get(0).get("id");
        assert documentId != null;
        UUID uuid = null;
        try{
            uuid = UUID.fromString(documentId);
            //do something
        } catch (IllegalArgumentException exception){
            //handle the case where string is not valid UUID
        }

        assert uuid != null;

        //2. includes samples
        assert SolrUtils.getRecordCount("cl620:*") == 5;
        assert SolrUtils.getRecordCount("cl927:*") == 5;
    }

    public void loadTestDataset(String datasetID, String inputPath) throws Exception {

        DwcaPipelineOptions dwcaOptions = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, new String[]{
                "--datasetId=" + datasetID,
                "--attempt=1",
                "--runner=SparkRunner",
                "--metaFileName=dwca-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
                "--inputPath=" + inputPath
        });
        DwcaToVerbatimPipeline.run(dwcaOptions);

        InterpretationPipelineOptions interpretationOptions = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=" + datasetID,
                "--attempt=1",
                "--runner=SparkRunner",
                "--interpretationTypes=ALL",
                "--metaFileName=interpretation-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
                "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
                "--properties=src/test/resources/pipelines.yaml",
                "--useExtendedRecordId=true"
        });
        ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

        InterpretationPipelineOptions uuidOptions = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=" + datasetID,
                "--attempt=1",
                "--runner=SparkRunner",
                "--metaFileName=uuid-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
                "--inputPath=/tmp/la-pipelines-test/complete-pipeline/dr893/1/verbatim.avro",
                "--properties=src/test/resources/pipelines.yaml",
                "--useExtendedRecordId=true"
        });
        ALAUUIDMintingPipeline.run(uuidOptions);

        //export lat lngs
        InterpretationPipelineOptions latLngOptions = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=" + datasetID,
                "--attempt=1",
                "--runner=SparkRunner",
                "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
                "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
                "--properties=src/test/resources/pipelines.yaml"
        });
        ALAInterpretedToLatLongCSVPipeline.run(latLngOptions);

        //sample
        LayerCrawler.run(latLngOptions);

        //sample -> avro
        ALASamplingToAvroPipeline.run(latLngOptions);

        //solr
        ALASolrPipelineOptions solrOptions = PipelinesOptionsFactory.create(ALASolrPipelineOptions.class, new String[]{
                "--datasetId=" + datasetID,
                "--attempt=1",
                "--runner=SparkRunner",
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
