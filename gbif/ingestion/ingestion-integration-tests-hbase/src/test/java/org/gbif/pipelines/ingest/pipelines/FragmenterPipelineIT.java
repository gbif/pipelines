package org.gbif.pipelines.ingest.pipelines;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.pipelines.fragmenter.common.TableAssert;
import org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.junit.After;
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

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;
  private static final String ID = "777";
  public static final String PROPERTIES_PATH = "data7/ingest/pipelines.yaml";

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private final Path outputFile = Paths.get(getClass().getResource("/").getFile()).resolve("dwca");
  private final Path properties = Paths.get(getClass().getResource("/data7/ingest").getFile());

  @Before
  public void before() {
    updateZkProperties();
  }

  @After
  public void after() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void pipelineTest() throws Exception {

    // State
    int expSize = 1;

    int attempt = 1;
    String datasetKey = UUID.randomUUID().toString();
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    createVerbatimAvro(options, String.valueOf(attempt), String.valueOf(attempt));

    // When
    FragmenterPipeline.run(options, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void repeatPipelineTest() throws Exception {

    // State
    int expSize = 1;

    int attempt = 2;
    int attempt2 = 3;
    String datasetKey = UUID.randomUUID().toString();
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    createVerbatimAvro(options, String.valueOf(attempt), String.valueOf(attempt));

    // When
    FragmenterPipeline.run(options, opt -> p);

    // State
    String[] args2 = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt2,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options2 = PipelinesOptionsFactory.createInterpretation(args2);
    // new file but the same data
    createVerbatimAvro(options2, String.valueOf(attempt), String.valueOf(attempt));

    // When
    FragmenterPipeline.run(options2, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt, endpointType);
  }

  @Test
  public void newDataPipelineTest() throws Exception {

    // State
    int expSize = 1;

    int attempt = 4;
    int attempt2 = 5;
    String datasetKey = UUID.randomUUID().toString();
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    // new file but the same data
    createVerbatimAvro(options, String.valueOf(attempt), String.valueOf(attempt));

    // When
    FragmenterPipeline.run(options, opt -> p);

    // State
    String[] args2 = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt2,
      "--runner=SparkRunner",
      "--metaFileName=fragmenter.yml",
      "--targetPath=" + outputFile,
      "--properties=" + properties + "/pipelines.yaml",
      "--testMode=true"
    };

    InterpretationPipelineOptions options2 = PipelinesOptionsFactory.createInterpretation(args2);
    // new file but the ids, new data
    createVerbatimAvro(options2, String.valueOf(attempt), String.valueOf(attempt2));

    // When
    FragmenterPipeline.run(options2, opt -> p);

    // Should
    TableAssert.assertTable(
        HBASE_SERVER.getConnection(), expSize, datasetKey, attempt2, endpointType);
  }

  @SneakyThrows
  private void createVerbatimAvro(InterpretationPipelineOptions options, String ids, String value) {
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

      coreTerm.put(DwcTerm.occurrenceID.qualifiedName(), ids);
      coreTerm.put(DwcTerm.institutionCode.qualifiedName(), ids);
      coreTerm.put(DwcTerm.collectionCode.qualifiedName(), ids);
      coreTerm.put(DwcTerm.catalogNumber.qualifiedName(), ids);

      coreTerm.put(DwcTerm.recordedBy.qualifiedName(), value);

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder().setCoreTerms(coreTerm).setId(ids).setExtensions(ext).build();
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

  @SneakyThrows
  private void updateZkProperties() {
    // create props
    PipelinesConfig config;
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    mapper.findAndRegisterModules();

    File resource =
        Paths.get(
                Thread.currentThread().getContextClassLoader().getResource(PROPERTIES_PATH).toURI())
            .toFile();
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_PATH)) {
      config = mapper.readValue(in, PipelinesConfig.class);
      config.setZkConnectionString(HBASE_SERVER.getZkQuorum());
    }

    // write properties to the file
    try (FileOutputStream out = new FileOutputStream(resource)) {
      mapper.writeValue(out, config);
    }
  }
}
