package au.org.ala.pipelines.beam;

import static org.junit.Assert.*;

import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class SensitiveDataPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  /** Tests for SOLR index creation. */
  @Test
  public void testSensitivePipeline() throws Exception {

    File shapefiles = new File("/tmp/pipelines-shp");
    assertTrue(
        "The shapefiles "
            + shapefiles
            + " should be loaded. If you are running this test standalone use mvn pre-integration-test",
        shapefiles.exists());
    // clear up previous test runs
    File pipeline = new File("/tmp/la-pipelines-test/sensitive-pipeline");
    FileUtils.deleteQuietly(pipeline);

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/sensitive-data/dr893");

    File dr = new File(pipeline, "dr893/1");
    File interpreted = new File(dr, "occurrence");
    assertTrue(interpreted.exists());
    File ala_sensitive_data = new File(interpreted, "ala_sensitive_data");
    assertTrue(ala_sensitive_data.exists());

    // Check correctly stated sensitivity
    Map<String, ALASensitivityRecord> sds =
        AvroReader.readRecords(
            HdfsConfigs.nullConfig(),
            ALASensitivityRecord.class,
            ala_sensitive_data.getPath() + "/*.avro");
    ALASensitivityRecord sds1 = sds.get("not-an-uuid-1");
    assertNotNull(sds1);
    assertTrue(sds1.getIsSensitive());
    assertEquals("generalised", sds1.getSensitive());
    assertEquals(
        "Record is Australia in Endangered. Generalised to 10km by Birds Australia.",
        sds1.getDataGeneralizations());
    assertEquals("149.4", sds1.getAltered().get("http://rs.tdwg.org/dwc/terms/decimalLongitude"));
    ALASensitivityRecord sds2 = sds.get("not-an-uuid-2");
    assertNotNull(sds2);
    assertFalse(sds2.getIsSensitive());
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
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline/dr893/1/verbatim.avro",
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
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    // sensitive data
    InterpretationPipelineOptions sensitiveOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--inputPath=/tmp/la-pipelines-test/sensitive-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    ALAInterpretedToSensitivePipeline.run(sensitiveOptions);
  }
}
