package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.ElasticUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class CompleteEventPipelineTestIT {

  IntegrationTestUtils itUtils;

  @Before
  public void setup() throws Exception {
    // clear up previous test runs
    itUtils = IntegrationTestUtils.getInstance();
    itUtils.setup();
  }

  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-event-pipeline"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr18391", absolutePath + "/complete-event-pipeline/dr18391");

    // wait for autocommit
    ElasticUtils.refreshIndex();

    long eventCount = ElasticUtils.getRecordCount("type", "event");
    assertEquals(5, eventCount);

    long occurrencesCount = ElasticUtils.getRecordCount("type", "occurrence");
    assertEquals(2, occurrencesCount);

    long allCount = ElasticUtils.getRecordCount();
    assertEquals(7, allCount);
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
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
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
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline/dr18391/1/verbatim.avro",
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

    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    // run verbatim to event pipeline
    InterpretationPipelineOptions verbatimEventOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--targetPath=/tmp/la-pipelines-test/complete-event-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-event-pipeline/dr18391/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    ALAVerbatimToEventPipeline.run(verbatimEventOptions);

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
          "--config=" + itUtils.getPropertiesFilePath()
        });
  }
}
