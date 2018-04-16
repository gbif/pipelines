package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline;
import org.gbif.pipelines.assembling.utils.HdfsUtils;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.InterpretationType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.junit.Test;

/**
 * Tests the {@link GbifInterpretationPipeline}.
 */
public class GbifInterpretationPipelineTest {

  private static final String INPUT = "avro/extendedRecords*";
  private static final String OUTPUT = "output";

  private static Configuration configuration = new Configuration();
  private static MiniDFSCluster hdfsCluster;
  private static FileSystem fs;
  private static URI hdfsClusterBaseUri;
  private static MockWebServer mockServer;

  @BeforeClass
  public static void setUp() throws Exception {
    // mini cluster
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);

    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    hdfsCluster = new MiniDFSCluster.Builder(configuration).build();
    fs = FileSystem.newInstance(configuration);
    hdfsClusterBaseUri = new URI(configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/");

    // mock server
    mockServer = new MockWebServer();
    // TODO: check if the port is in use??
    mockServer.start(1111);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    hdfsCluster.shutdown();
    mockServer.shutdown();
  }

  @Test
  public void temporalInterpretationTest() throws IOException {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setInputFile(INPUT);
    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");
    options.setInterpretationTypes(Collections.singletonList(InterpretationType.TEMPORAL));

    Pipeline pipeline = GbifInterpretationPipeline.newInstance(options).createPipeline();

    PipelineResult.State state = pipeline.run().waitUntilFinish();

    Assert.assertEquals(PipelineResult.State.DONE, state);

    // check dataset dir
    checkDirCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                   options.getDatasetId()).toString()), 1);

    // check interpretation dir
    checkInterpretationFiles(options, InterpretationType.TEMPORAL);

    fs.delete(HdfsUtils.buildPath(options.getDefaultTargetDirectory()), true);
  }

  @Test
  public void multipleInterpretationsTest() throws IOException {
    enqueueGeocodeResponse();

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    List<InterpretationType> interpretations =
      Arrays.asList(InterpretationType.COMMON, InterpretationType.TEMPORAL, InterpretationType.LOCATION);

    options.setInputFile(INPUT);
    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");
    options.setInterpretationTypes(interpretations);

    Pipeline pipeline = GbifInterpretationPipeline.newInstance(options).createPipeline();

    PipelineResult.State state = pipeline.run().waitUntilFinish();

    Assert.assertEquals(PipelineResult.State.DONE, state);

    // check dataset dir
    checkDirCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                   options.getDatasetId()).toString()),
                    interpretations.size());

    // check interpretation dirs
    for (InterpretationType interpretationType : interpretations) {
      checkInterpretationFiles(options, interpretationType);
    }

    fs.delete(HdfsUtils.buildPath(options.getDefaultTargetDirectory()), true);
  }

  @Test
  public void nullInterpretationParamTest() throws IOException {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setInputFile(INPUT);
    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");

    Pipeline pipeline = GbifInterpretationPipeline.newInstance(options).createPipeline();

    PipelineResult.State state = pipeline.run().waitUntilFinish();

    Assert.assertEquals(PipelineResult.State.DONE, state);

    // check dataset dir
    checkDirCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                   options.getDatasetId()).toString()),
                    InterpretationType.ALL_INTERPRETATIONS.size());

    fs.delete(HdfsUtils.buildPath(options.getDefaultTargetDirectory()), true);
  }

  @Test(expected = NullPointerException.class)
  public void nullInputTest() {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");

    GbifInterpretationPipeline.newInstance(options).createPipeline();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullDatasetIdTest() {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);

    GbifInterpretationPipeline.newInstance(options).createPipeline();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullHdfsConfigTest() {
    DataProcessingPipelineOptions options = PipelineOptionsFactory.as(DataProcessingPipelineOptions.class);

    options.setInputFile(INPUT);
    options.setDefaultTargetDirectory(hdfsClusterBaseUri + OUTPUT);
    options.setDatasetId("123");

    GbifInterpretationPipeline.newInstance(options).createPipeline();
  }

  /**
   * Checks that the creation of expected directories and files for an interpretation was correct.
   */
  private void checkInterpretationFiles(
    DataProcessingPipelineOptions options, InterpretationType interpretationType
  ) throws IOException {
    {
      // check interpretation DIR
      checkDirCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                     options.getDatasetId(),
                                                                     interpretationType.name().toLowerCase())
                                                   .toString()), 2);
      // check interpretation AVRO FILE
      checkAvroFileCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                          options.getDatasetId(),
                                                                          interpretationType.name().toLowerCase(),
                                                                          "interpreted.avro").toString()));
      // check ISSUES DIR
      checkDirCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                     options.getDatasetId(),
                                                                     interpretationType.name().toLowerCase(),
                                                                     "issues").toString()), 1);
      // check ISSUES AVRO FILE
      checkAvroFileCreated(hdfsClusterBaseUri.resolve(HdfsUtils.buildPath(options.getDefaultTargetDirectory(),
                                                                          options.getDatasetId(),
                                                                          interpretationType.name().toLowerCase(),
                                                                          "issues",
                                                                          "issues.avro").toString()));
    }
  }

  /**
   * Checks that the creation of an expected directory was correct.
   */
  private void checkDirCreated(URI uri, int expectedFilesInDir) throws IOException {
    FileStatus[] fileStatusesDir = fs.globStatus(new Path(uri.toString()));
    Assert.assertNotNull(fileStatusesDir);
    Assert.assertEquals(1, fileStatusesDir.length);

    FileStatus dirStatus = fileStatusesDir[0];
    Assert.assertTrue(dirStatus.isDirectory());
    Assert.assertTrue(fs.exists(dirStatus.getPath()));

    // there should be only the issues avro file
    Assert.assertEquals(expectedFilesInDir, fs.listStatus(dirStatus.getPath()).length);
  }

  /**
   * Checks that the creation of an expected avro file was correct.
   */
  private void checkAvroFileCreated(URI uri) throws IOException {
    FileStatus[] fileStatusesAvro = fs.globStatus(new Path(uri.toString()));
    Assert.assertNotNull(fileStatusesAvro);
    Assert.assertTrue(fileStatusesAvro.length > 0);

    for (FileStatus fileStatus : fileStatusesAvro) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(fs.exists(fileStatus.getPath()));
    }
  }

  /**
   * Enqueus a geocode response in the mock server.
   */
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
