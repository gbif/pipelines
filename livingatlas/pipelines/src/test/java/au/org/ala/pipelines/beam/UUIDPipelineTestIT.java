package au.org.ala.pipelines.beam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.AvroUtils;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class UUIDPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  /** Test the generation of UUIDs for datasets that are use non-DwC terms for unique key terms */
  @Test
  public void testNonDwC() throws Exception {
    // dr1864 - has deviceId
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr1864", absolutePath + "/uuid-management/dr1864");

    Map<String, String> uniqueKeyToUUID =
        AvroUtils.readKeysForPath(
            "/tmp/la-pipelines-test/uuid-management/dr1864/1/identifiers/ala_uuid/interpret-*");

    // check generated keys are present
    assertTrue(uniqueKeyToUUID.containsKey("dr1864|2|12/12/01"));
    assertTrue(uniqueKeyToUUID.containsKey("dr1864|3|12/12/01"));
  }

  /**
   * Tests for UUID creation. This test simulates a dataset being:
   *
   * <pre>
   *   1) Loaded
   *   2) Re-loaded
   *   3) Re-loaded with records removed
   *   4) Re-loaded with removed records being added back & UUID being preserved.
   * </pre>
   */
  @Test
  public void testUuidsPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/uuid-management"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/uuid-management/dr893");

    // validation function
    Map<String, String> keysAfterFirstRun =
        AvroUtils.readKeysForPath(
            "/tmp/la-pipelines-test/uuid-management/dr893/1/identifiers/ala_uuid/interpret-*");
    assertEquals(5, keysAfterFirstRun.size());

    // Step 2: Check UUIDs where preserved
    loadTestDataset("dr893", absolutePath + "/uuid-management/dr893");
    Map<String, String> keysAfterSecondRun =
        AvroUtils.readKeysForPath(
            "/tmp/la-pipelines-test/uuid-management/dr893/1/identifiers/ala_uuid/interpret-*");

    // validate
    assertEquals(keysAfterFirstRun.size(), keysAfterSecondRun.size());
    for (Map.Entry<String, String> key : keysAfterFirstRun.entrySet()) {
      assertTrue(keysAfterSecondRun.containsKey(key.getKey()));
      assertEquals(keysAfterSecondRun.get(key.getKey()), key.getValue());
    }

    // Step 3: Check UUIDs where preserved for the removed records
    loadTestDataset("dr893", absolutePath + "/uuid-management/dr893-reduced");
    Map<String, String> keysAfterThirdRun =
        AvroUtils.readKeysForPath(
            "/tmp/la-pipelines-test/uuid-management/dr893/1/identifiers/ala_uuid/interpret-*");
    // validate
    for (Map.Entry<String, String> key : keysAfterThirdRun.entrySet()) {
      assertTrue(keysAfterFirstRun.containsKey(key.getKey()));
      assertEquals(keysAfterFirstRun.get(key.getKey()), key.getValue());
    }

    // Step 4: Check UUIDs where preserved for the re-added records
    loadTestDataset("dr893", absolutePath + "/uuid-management/dr893-readded");
    Map<String, String> keysAfterFourthRun =
        AvroUtils.readKeysForPath(
            "/tmp/la-pipelines-test/uuid-management/dr893/1/identifiers/ala_uuid/interpret-*");
    assertEquals(6, keysAfterFourthRun.size());
    // validate
    for (Map.Entry<String, String> key : keysAfterFirstRun.entrySet()) {
      assertTrue(keysAfterFourthRun.containsKey(key.getKey()));
      assertEquals(keysAfterFourthRun.get(key.getKey()), key.getValue());
    }
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
              "--targetPath=/tmp/la-pipelines-test/uuid-management",
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
              "--targetPath=/tmp/la-pipelines-test/uuid-management",
              "--inputPath=/tmp/la-pipelines-test/uuid-management/"
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
              "--targetPath=/tmp/la-pipelines-test/uuid-management",
              "--inputPath=/tmp/la-pipelines-test/uuid-management",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check the UUIDs validate
    Assert.assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());
  }
}
