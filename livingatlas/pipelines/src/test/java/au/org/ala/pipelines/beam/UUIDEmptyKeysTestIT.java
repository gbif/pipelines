package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class UUIDEmptyKeysTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  @Before
  public void setup() throws Exception {
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/uuid-empty-terms"));
  }

  /** Test the generation of UUIDs for datasets that are use non-DwC terms for unique key terms */
  @Test
  public void testEmptyKeys() throws Exception {
    // dr1864 - has deviceId
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/uuid-empty-terms/dr893");
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--pipelineStep=DWCA_TO_VERBATIM",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/uuid-empty-terms",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/uuid-empty-terms",
              "--inputPath=/tmp/la-pipelines-test/uuid-empty-terms/"
                  + datasetID
                  + "/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/uuid-empty-terms",
              "--inputPath=/tmp/la-pipelines-test/uuid-empty-terms/",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });

    ALAUUIDValidationPipeline.run(uuidOptions);

    // assert count is 2
    assertEquals(1L, ValidationUtils.getInvalidRecordCount(uuidOptions).longValue());
    assertFalse(ValidationUtils.checkValidationFile(uuidOptions).getValid());
  }
}
