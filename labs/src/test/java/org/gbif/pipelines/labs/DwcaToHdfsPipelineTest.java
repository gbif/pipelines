package org.gbif.pipelines.labs;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.transform.TypeDescriptors;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.Interpretation;
import org.gbif.pipelines.config.TargetPath;
import org.gbif.pipelines.labs.functions.FunctionFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the class {@link DwcaToHdfsPipelineTest}.
 */
public class DwcaToHdfsPipelineTest {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToHdfsPipelineTest.class);

  private static final String DWCA_FILE_PATH = "data/dwca.zip";

  private static MiniDFSCluster hdfsCluster;
  private static Configuration configuration = new Configuration();
  private static FileSystem fs;
  private static URI hdfsClusterBaseUri;

  @BeforeClass
  public static void setUp() throws Exception {
    File baseDir = new File("./miniCluster/hdfs/").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    fs = FileSystem.newInstance(configuration);
    hdfsClusterBaseUri = new URI(configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "/");
  }

  @AfterClass
  public static void tearDown() {
    hdfsCluster.shutdown();
  }

  @Test
  public void givenHdfsClusterWhenWritingDwcaToHdfsThenFileCreated() throws Exception {

    // create options
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);
    options.setRunner(DirectRunner.class);

    options.setInputFile(DWCA_FILE_PATH);
    options.setDatasetId("123");
    options.setDefaultTargetDirectory(hdfsClusterBaseUri + "/pipelines");

    // create and run pipeline
    createAndRunPipeline(options);

    // test results
    URI uriTargetPath =
      hdfsClusterBaseUri.resolve(TargetPath.getFullPath(options.getDefaultTargetDirectory(), options.getDatasetId())
                                 + "*");
    FileStatus[] fileStatuses = fs.globStatus(new Path(uriTargetPath.toString()));

    Assert.assertNotNull(fileStatuses);
    Assert.assertTrue(fileStatuses.length > 0);

    // a bit redundant, just for demo purposes
    for (FileStatus fileStatus : fileStatuses) {
      Assert.assertTrue(fileStatus.isFile());
      Assert.assertTrue(fs.exists(fileStatus.getPath()));
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPipelineOptionsTest() {

    // create options
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);
    options.setRunner(DirectRunner.class);

    // create and run pipeline
    createAndRunPipeline(options);
  }

  @Test
  public void defaultTargetPathsTest() {
    // create options
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(configuration);

    Map<Interpretation, TargetPath> targetPaths = options.getTargetPaths();

    Assert.assertNotNull(targetPaths);
    Assert.assertEquals(Interpretation.values().length, targetPaths.size());

    for (Interpretation interpretation : Interpretation.values()) {
      TargetPath tp = targetPaths.get(interpretation);

      Assert.assertNotNull(tp);
      Assert.assertEquals(tp.getDirectory(), options.getDefaultTargetDirectory());
      Assert.assertEquals(tp.getFileName(), interpretation.getDefaultFileName());
    }

  }

  private void createAndRunPipeline(DataProcessingPipelineOptions options) {
    Objects.requireNonNull(options, "Pipeline options cannot be null");

    String targetPath = TargetPath.getFullPath(options.getDefaultTargetDirectory(), options.getDatasetId());

    LOG.info("Target path : {}", targetPath);

    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, UntypedOccurrence.class);

    // temp dir for Dwca
    String tmpDirDwca = new File(options.getInputFile()).getParentFile().getPath() + File.separator + "tmpDwca";

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords =
      pipeline.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths(options.getInputFile(), tmpDirDwca));

    // TODO: Explore the generics as to why the coder registry does not find it and we need to set the coder explicitly
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "Convert the objects into untyped DwC style records",
      MapElements.into(TypeDescriptors.untypedOccurrence()).via(FunctionFactory.untypedOccurrenceBuilder()))
      .setCoder(AvroCoder.of(UntypedOccurrence.class));

    verbatimRecords.apply("Write Avro files",
                          AvroIO.write(UntypedOccurrence.class)
                            .to(targetPath)
                            .withTempDirectory(FileSystems.matchNewResource(options.getHdfsTempLocation(), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
