package org.gbif.pipelines.ingest.pipelines;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.ingest.pipelines.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("all")
@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class VerbatimToInterpretedPipelineIT {

  private static final String ID = "777";
  private static final String DATASET_KEY = UUID.randomUUID().toString();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void indexingPipelineTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "1";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-interpreted.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--testMode=true"
    };

    // State
    String pipelinesProperties = outputFile + "/pipelines.yaml";
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    // Create varbatim.avro
    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(options, VerbatimTransform.create(), ID)) {

      Map<String, String> core = new HashMap<>();
      core.put(DwcTerm.datasetID.qualifiedName(), "datasetID");
      core.put(DwcTerm.institutionID.qualifiedName(), "institutionID");
      core.put(DwcTerm.datasetName.qualifiedName(), "datasetName");
      core.put(DwcTerm.eventID.qualifiedName(), "eventID");
      core.put(DwcTerm.parentEventID.qualifiedName(), "parentEventID");
      core.put(DwcTerm.samplingProtocol.qualifiedName(), "samplingProtocol");

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder()
              .setId(ID)
              .setCoreRowType(DwcTerm.Event.qualifiedName())
              .setCoreTerms(core)
              .build();

      writer.append(extendedRecord);
    }
    Path from =
        Paths.get(outputFile, DATASET_KEY, attempt, "interpreted/verbatim/interpret-777.avro");
    Path to = Paths.get(outputFile, DATASET_KEY, attempt, "verbatim.avro");
    Files.deleteIfExists(to);
    Files.move(from, to);

    // When
    VerbatimToInterpretedPipeline.run(options, opt -> p);

    // Shoud
    String metricsOutput =
        String.join("/", outputFile, DATASET_KEY, attempt, "verbatim-to-interpreted.yml");
    assertTrue(Files.exists(Paths.get(metricsOutput)));

    String interpretedOutput = String.join("/", outputFile, DATASET_KEY, attempt, "interpreted");

    assertEquals(3, new File(interpretedOutput).listFiles().length);
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier");
    assertFile(EventCoreRecord.class, interpretedOutput + "/event_core");
    assertFile(ExtendedRecord.class, interpretedOutput + "/verbatim");
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
      Assert.assertTrue(dataFileReader.hasNext());
      while (dataFileReader.hasNext()) {
        T record = dataFileReader.next();
        Assert.assertNotNull(record);

        String id = (String) record.get("id");
        if (record instanceof MetadataRecord) {
          Assert.assertEquals(DATASET_KEY, id);
        } else {
          Assert.assertEquals(ID, id);
        }
      }
    }
  }
}
