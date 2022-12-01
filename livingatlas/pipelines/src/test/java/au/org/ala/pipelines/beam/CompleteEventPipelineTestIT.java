package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.options.DwcaToVerbatimPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.ElasticUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class CompleteEventPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();
  public static final String INDEX_NAME = "complete_event_it";

  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-event-pipeline"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr18391", absolutePath + "/complete-event-pipeline/dr18391");

    // wait for autocommit
    ElasticUtils.refreshIndex(INDEX_NAME);

    long eventCount = ElasticUtils.getRecordCount(INDEX_NAME, "type", "event");
    assertEquals(5, eventCount);

    long occurrencesCount = ElasticUtils.getRecordCount(INDEX_NAME, "type", "occurrence");
    assertEquals(2, occurrencesCount);

    long allCount = ElasticUtils.getRecordCount(INDEX_NAME);
    assertEquals(7, allCount);
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaToVerbatimPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaToVerbatimPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=" + inputPath
            });
    ALADwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline/dr18391/1/verbatim/*.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
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
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    ALAInterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    String esSchemaPath =
        new File("src/test/resources/complete-event-pipeline/es-event-core-schema.json")
            .getAbsolutePath();

    // run event to ES pipeline
    ALAEventToEsIndexPipeline.main(
        new String[] {
          "--datasetId=" + datasetID,
          "--attempt=1",
          "--runner=SparkRunner",
          "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
          "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline",
          "--esSchemaPath=" + esSchemaPath,
          "--esAlias=" + INDEX_NAME,
          "--esIndexName=" + INDEX_NAME + "_" + datasetID,
          "--config=" + itUtils.getPropertiesFilePath()
        });
  }
}
