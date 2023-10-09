package org.gbif.pipelines.ingest.pipelines;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter;
import org.gbif.pipelines.ingest.resources.HbaseServer;
import org.gbif.pipelines.ingest.resources.TableAssert;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
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

  private static final String DATASET_KEY = "50c9509d-22c7-4a22-a47d-8c48425ef4a8";
  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;
  private static final String ID = "777";
  private final Path outputFile = Paths.get(getClass().getResource("/dwca").getFile());
  private final Path properties = Paths.get(getClass().getResource("/data7/ingest").getFile());

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void pipelineTest() throws Exception {

    // State
    int expSize = 210;
    int attempt = 231;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    createVerbatimAvro(options, "attempt_" + attempt);

    // When
    FragmenterPipeline.run(options, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, DATASET_KEY, attempt, endpointType);
  }

  @Test
  public void repeatPipelineTest() throws Exception {

    // State
    int expSize = 0;

    int attempt = 231;
    int attempt2 = 232;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    createVerbatimAvro(options, "attempt_" + attempt);

    // When
    FragmenterPipeline.run(options, opt -> p);

    // State
    String[] args2 = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt2,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options2 = PipelinesOptionsFactory.createInterpretation(args2);

    // When
    FragmenterPipeline.run(options2, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, DATASET_KEY, attempt2, endpointType);
  }

  @Test
  public void newDataPipelineTest() throws Exception {

    // State
    int expSize = 0;

    int attempt = 231;
    int attempt2 = 232;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    createVerbatimAvro(options, "attempt_" + attempt);

    // When
    FragmenterPipeline.run(options, opt -> p);

    // State
    String[] args2 = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt2,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options2 = PipelinesOptionsFactory.createInterpretation(args2);

    // When
    createVerbatimAvro(options, "attempt_" + attempt2);

    // When
    FragmenterPipeline.run(options2, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, DATASET_KEY, attempt2, endpointType);
  }

  @SneakyThrows
  private void createVerbatimAvro(InterpretationPipelineOptions options, String value) {
    // Create varbatim.avro
    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            options, VerbatimTransform.create(), CORE_TERM, ID)) {

      Map<String, String> ext1 = new HashMap<>(5);
      ext1.put(DwcTerm.measurementID.qualifiedName(), value);
      ext1.put(DwcTerm.measurementType.qualifiedName(), value);
      ext1.put(DwcTerm.measurementValue.qualifiedName(), value);
      ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), value);
      ext1.put(DwcTerm.measurementUnit.qualifiedName(), value);

      Map<String, List<Map<String, String>>> ext = new HashMap<>();
      ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

      Map<String, String> coreTerm = new HashMap<>(5);
      coreTerm.put(DwcTerm.occurrenceID.qualifiedName(), value);
      coreTerm.put(DwcTerm.institutionCode.qualifiedName(), value);
      coreTerm.put(DwcTerm.collectionCode.qualifiedName(), value);
      coreTerm.put(DwcTerm.catalogNumber.qualifiedName(), value);
      coreTerm.put(DwcTerm.recordedBy.qualifiedName(), value);

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder().setCoreTerms(coreTerm).setId(ID).setExtensions(ext).build();
      writer.append(extendedRecord);
    }

    String datasetId = options.getDatasetId();
    String attempt = options.getAttempt().toString();
    String outputFileString = outputFile.toString();
    Path from =
        Paths.get(outputFileString, datasetId, attempt, "occurrence/verbatim/interpret-777.avro");
    Path to = Paths.get(outputFileString, datasetId, attempt, "verbatim.avro");
    Files.deleteIfExists(to);
    Files.move(from, to);
  }
}
