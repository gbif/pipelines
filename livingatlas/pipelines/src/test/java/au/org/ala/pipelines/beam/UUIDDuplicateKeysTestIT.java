package au.org.ala.pipelines.beam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToVerbatimPipeline;
import org.junit.Before;
import org.junit.Test;

public class UUIDDuplicateKeysTestIT {

  @Before
  public void setup() throws Exception {
    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/uuid-duplicate-keys"));
  }

  /** Test the generation of UUIDs for datasets that are use non-DwC terms for unique key terms */
  @Test
  public void testDuplicateKeys() throws Exception {
    // dr1864 - has deviceId
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/uuid-duplicate-keys/dr893");
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
              "--metaFileName=dwca-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/uuid-duplicate-keys",
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
              "--targetPath=/tmp/la-pipelines-test/uuid-duplicate-keys",
              "--inputPath=/tmp/la-pipelines-test/uuid-duplicate-keys/"
                  + datasetID
                  + "/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
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
              "--metaFileName=uuid-metrics.yml",
              "--targetPath=/tmp/la-pipelines-test/uuid-duplicate-keys",
              "--inputPath=/tmp/la-pipelines-test/uuid-duplicate-keys/"
                  + datasetID
                  + "/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });

    ALAUUIDValidationPipeline.run(uuidOptions);

    // assert count is 2
    assertEquals(2L, ValidationUtils.getDuplicateKeyCount(uuidOptions));
    assertFalse(ValidationUtils.checkValidationFile(uuidOptions));
  }
}
