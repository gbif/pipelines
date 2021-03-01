package au.org.ala.pipelines.beam;

import au.org.ala.pipelines.options.ImageServicePipelineOptions;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import java.io.File;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ImageServicePipelineTestIT {

  MockWebServer server;

  @After
  public void teardown() throws Exception {
    server.shutdown();
  }

  @Before
  public void setup() throws Exception {
    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/image-service"));
    server = TestUtils.createMockCollectory();
    server.start(3939);
  }

  /** Test the generation of UUIDs for datasets that are use non-DwC terms for unique key terms */
  @Test
  public void testNonDwC() {
    // dr1864 - has deviceId
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    // Step 1: load a dataset and verify all records have a UUID associated
    loadTestDataset("dr893", absolutePath + "/image-service/dr893", "image-service");
  }

  public void loadTestDataset(String datasetID, String inputPath, String testDir) {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--pipelineStep=DWCA_TO_VERBATIM",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
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
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
              "--inputPath=/tmp/la-pipelines-test/"
                  + testDir
                  + "/"
                  + datasetID
                  + "/1/verbatim.avro",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    ImageServicePipelineOptions imageOptions =
        PipelinesOptionsFactory.create(
            ImageServicePipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.IMAGE_SERVICE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/" + testDir,
              "--inputPath=/tmp/la-pipelines-test/" + testDir,
              "--properties=" + TestUtils.getPipelinesConfigFile()
            });

    String absolutePath = new File("src/test/resources").getAbsolutePath();
    String imageServiceExportPath =
        absolutePath + "/" + testDir + "/" + datasetID + "/image-service-export.csv";

    ImageServiceSyncPipeline.run(imageOptions, imageServiceExportPath);
  }
}
