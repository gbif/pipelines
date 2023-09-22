package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.ingest.resources.ZkServer;
import org.gbif.pipelines.ingest.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@SuppressWarnings("all")
@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
@FixMethodOrder(MethodSorters.JVM)
public class VerbatimToIdentifierPipelineIT {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;
  private static final String ID = "777";

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();

  @Test
  public void identifierInterpretationTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String datasetKey = UUID.randomUUID().toString();
    String attempt = "78";

    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-identifier.yml",
      "--inputPath=" + outputFile + "/" + datasetKey + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--interpretationTypes=" + IDENTIFIER,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--generateIds=false",
      "--testMode=true"
    };

    String pipelinesProperties = outputFile + "/pipelines.yaml";

    // Add vocabulary
    Path pipelinesPropertiesPath = Paths.get(pipelinesProperties);
    List<String> lines = Files.readAllLines(pipelinesPropertiesPath);

    boolean anyMatch = lines.stream().anyMatch(x -> x.startsWith("  vocabulariesPath"));

    if (!anyMatch) {
      String vocabulariesPath = "  vocabulariesPath: " + outputFile;
      lines.add(vocabulariesPath);
      Files.write(pipelinesPropertiesPath, lines);
    }

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    // Create varbatim.avro
    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            options, VerbatimTransform.create(), CORE_TERM, ID)) {

      Map<String, String> core = new HashMap<>();
      core.put(DwcTerm.identifiedBy.qualifiedName(), "Id1");

      Map<String, String> ext1 = new HashMap<>();
      ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
      ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");

      Map<String, List<Map<String, String>>> ext = new HashMap<>();
      ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder().setId(ID).setCoreTerms(core).setExtensions(ext).build();
      writer.append(extendedRecord);
    }
    Path from =
        Paths.get(outputFile, datasetKey, attempt, "occurrence/verbatim/interpret-777.avro");
    Path to = Paths.get(outputFile, datasetKey, attempt, "verbatim.avro");
    Files.deleteIfExists(to);
    Files.move(from, to);

    // When
    VerbatimToIdentifierPipeline.run(options);

    // Shoud
    String metricsOutput =
        String.join("/", outputFile, datasetKey, attempt, "verbatim-to-identifier.yml");
    assertTrue(Files.exists(Paths.get(metricsOutput)));

    String interpretedOutput = String.join("/", outputFile, datasetKey, attempt, "occurrence");

    long dirCount =
        Arrays.stream(new File(interpretedOutput).listFiles())
            .filter(x -> !x.getPath().contains("verbatim"))
            .count();

    assertEquals(3L, dirCount);
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier");
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier_absent");
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier_invalid");
  }

  private <T extends SpecificRecordBase> void assertFile(Class<T> clazz, String output)
      throws Exception {

    Assert.assertTrue(Files.exists(Paths.get(output)));

    File file =
        Arrays.stream(new File(output).listFiles())
            .filter(x -> x.toString().endsWith(".avro"))
            .findAny()
            .get();

    Assert.assertTrue(file.exists());

    DatumReader<T> ohrDatumReader = new SpecificDatumReader<>(clazz);
    try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, ohrDatumReader)) {
      while (dataFileReader.hasNext()) {
        T record = dataFileReader.next();
        Assert.assertNotNull(record);

        String id = (String) record.get("id");
        Assert.assertEquals(ID, id);
      }
    }
  }
}
