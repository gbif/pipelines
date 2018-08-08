package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.GbifInterpretationType;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.utils.FsUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.PipelineResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IntepretationPipelineTest {

  private static final String INPUT = "src/test/resources/avro/extendedRecords*";
  private static final String OUTPUT = "output";

  private static final Configuration configuration = new Configuration();
  private static MiniDFSCluster hdfsCluster;
  private static FileSystem fs;
  private static URI hdfsClusterBaseUri;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @ClassRule public static final MockWebServer mockServer = new MockWebServer();

  @BeforeClass
  public static void setUp() throws IOException, URISyntaxException {
    // mini cluster
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);

    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    hdfsCluster = new MiniDFSCluster.Builder(configuration).build();
    fs = FileSystem.newInstance(configuration);
    hdfsClusterBaseUri =
        new URI(configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/");
  }

  @AfterClass
  public static void tearDown() {
    hdfsCluster.shutdown();
  }

  /**
   * This test doesn't use any interpretation that requires a WS call. Therefore, neither the ws
   * properties path or the mock pipeline with the mock server config are provided and the pipeline
   * is expected to work.
   */
  @Test
  public void temporalInterpretationTest() throws IOException {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);
    options.setInputPath(INPUT);
    options.setTargetPath(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");
    options.setAttempt(1);
    options.setInterpretationTypes(
        Collections.singletonList(GbifInterpretationType.TEMPORAL.name()));

    PipelineResult.State state = InterpretationPipeline.create(options).run();

    Assert.assertEquals(PipelineResult.State.DONE, state);

    // check dataset dir
    checkDirCreated(
        hdfsClusterBaseUri.resolve(
            FsUtils.buildPathString(
                options.getTargetPath(), options.getDatasetId(), options.getAttempt().toString())),
        1);

    // check interpretation dir
    checkInterpretationFiles(options, GbifInterpretationType.TEMPORAL.name());

    // delete files created to leave the FS clean for other tests
    fs.delete(FsUtils.buildPath(options.getTargetPath()), true);
  }

  @Test
  public void multipleInterpretationsTest() throws IOException {
    enqueueGeocodeResponse();

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    List<String> interpretations =
        Arrays.asList(GbifInterpretationType.COMMON.name(), GbifInterpretationType.TEMPORAL.name());

    options.setInputPath(INPUT);
    options.setTargetPath(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");
    options.setAttempt(1);
    options.setInterpretationTypes(interpretations);

    // we use a mock pipeline to use it with the interpretations that require a mock server
    PipelineResult.State state = InterpretationPipeline.create(options).run();

    Assert.assertEquals(PipelineResult.State.DONE, state);

    // check dataset dir
    checkDirCreated(
        hdfsClusterBaseUri.resolve(
            FsUtils.buildPathString(
                options.getTargetPath(), options.getDatasetId(), options.getAttempt().toString())),
        interpretations.size());

    // check interpretation dirs
    for (String interpretationType : interpretations) {
      checkInterpretationFiles(options, interpretationType);
    }

    // delete files created to leave the FS clean for other tests
    fs.delete(FsUtils.buildPath(options.getTargetPath()), true);
  }

  @Test(expected = NullPointerException.class)
  public void nullInputTest() {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setTargetPath(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");
    options.setAttempt(1);

    InterpretationPipeline.create(options).run();
  }

  @Test(expected = NullPointerException.class)
  public void nullDatasetIdTest() {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setTargetPath(hdfsClusterBaseUri + OUTPUT);

    InterpretationPipeline.create(options).run();
  }

  /**
   * Checks that the creation of expected directories and files for an interpretation was correct.
   */
  private void checkInterpretationFiles(DataProcessingPipelineOptions options, String type)
      throws IOException {

    String pathString =
        FsUtils.buildPathString(
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            type.toLowerCase());

    // check interpretation DIR
    checkDirCreated(hdfsClusterBaseUri.resolve(pathString), 2);
    // check interpretation AVRO FILE
    checkAvroFileCreated(
        hdfsClusterBaseUri.resolve(FsUtils.buildPathString(pathString, "interpreted*")));
    // check ISSUES DIR
    checkDirCreated(hdfsClusterBaseUri.resolve(FsUtils.buildPathString(pathString, "issues")), 1);
    // check ISSUES AVRO FILE
    checkAvroFileCreated(
        hdfsClusterBaseUri.resolve(FsUtils.buildPathString(pathString, "issues", "issues*")));
  }

  /** Checks that the creation of an expected directory was correct. */
  private void checkDirCreated(URI uri, int expectedFilesInDir) throws IOException {
    FileStatus[] fileStatusesDir = fs.globStatus(new Path(uri.toString()));
    Assert.assertNotNull(fileStatusesDir);
    Assert.assertEquals(1, fileStatusesDir.length);

    FileStatus dirStatus = fileStatusesDir[0];
    Assert.assertTrue(dirStatus.isDirectory());
    Assert.assertTrue(fs.exists(dirStatus.getPath()));

    // there should be only the issues avro file
    Assert.assertTrue(expectedFilesInDir <= fs.listStatus(dirStatus.getPath()).length);
  }

  /** Checks that the creation of an expected avro file was correct. */
  private void checkAvroFileCreated(URI uri) throws IOException {
    FileStatus[] fileStatusesAvro = fs.globStatus(new Path(uri.toString()));
    Assert.assertNotNull(fileStatusesAvro);
    Assert.assertTrue(fileStatusesAvro.length > 0);

    for (FileStatus fileStatus : fileStatusesAvro) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(fs.exists(fileStatus.getPath()));
    }
  }

  /** Enqueus a geocode response in the mock server. */
  private static void enqueueGeocodeResponse() {
    InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("denmark-reverse.json");
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    try {
      mockServer.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
