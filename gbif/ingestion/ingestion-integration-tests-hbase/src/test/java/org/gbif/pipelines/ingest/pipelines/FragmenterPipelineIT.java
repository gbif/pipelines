package org.gbif.pipelines.ingest.pipelines;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.resources.HbaseServer;
import org.gbif.pipelines.ingest.resources.TableAssert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class FragmenterPipelineIT {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path regularDwca = Paths.get(getClass().getResource("/dwca/regular").getFile());
  private final Path properties = Paths.get(getClass().getResource("/data7/ingest").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void pipelineTest() throws Exception {

    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--inputPath=" + regularDwca,
      "--targetPath=" + properties,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    FragmenterPipeline.run(options, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void repeatPipelineTest() throws Exception {

    // State
    int expSize = 210;
    String datasetKey = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
    int attempt = 231;
    int attempt2 = 232;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    // When
    String[] args = {
        "--datasetId=" + datasetKey,
        "--attempt=" + attempt,
        "--runner=SparkRunner",
        "--metaFileName=fragmenter.yml",
        "--inputPath=" + regularDwca,
        "--targetPath=" + properties,
        "--properties=" + properties + "/pipelines.yaml",
        "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    FragmenterPipeline.run(options, opt -> p);

    // When
    String[] args2 = {
        "--datasetId=" + datasetKey,
        "--attempt=" + attempt2,
        "--runner=SparkRunner",
        "--metaFileName=fragmenter.yml",
        "--inputPath=" + regularDwca,
        "--targetPath=" + properties,
        "--properties=" + properties + "/pipelines.yaml",
        "--testMode=true"
    };

    InterpretationPipelineOptions options2 = PipelinesOptionsFactory.createInterpretation(args2);
    FragmenterPipeline.run(options2, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }
}
