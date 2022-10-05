package org.gbif.pipelines.ingest.pipelines;

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
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.ingest.pipelines.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
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
public class VerbatimToEventPipelineIT {

  private static final DwcTerm CORE_TERM = DwcTerm.Event;

  private static final String ID = "777";
  private static final String ID2 = "888";
  private static final String DATASET_KEY = UUID.randomUUID().toString();

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void interpretationPipelineTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "1";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-occurrence.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--interpretationTypes=ALL",
      "--syncThreshold=0",
      "--testMode=true",
      "--dwcCore=Event"
    };
    // State
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
      core.put(DwcTerm.datasetID.qualifiedName(), "datasetID");
      core.put(DwcTerm.institutionID.qualifiedName(), "institutionID");
      core.put(DwcTerm.datasetName.qualifiedName(), "datasetName");
      core.put(DwcTerm.eventID.qualifiedName(), "eventID");
      core.put(DwcTerm.samplingProtocol.qualifiedName(), "samplingProtocol");
      core.put(GbifTerm.eventType.qualifiedName(), "Survey");

      Map<String, String> ext1 = new HashMap<>();
      ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
      ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
      ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
      ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
      ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
      ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
      ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
      ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
      ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

      Map<String, String> core2 = new HashMap<>();
      core2.put(DwcTerm.datasetID.qualifiedName(), "datasetID");
      core2.put(DwcTerm.institutionID.qualifiedName(), "institutionID");
      core2.put(DwcTerm.datasetName.qualifiedName(), "datasetName");
      core2.put(DwcTerm.eventID.qualifiedName(), "eventID2");
      core2.put(DwcTerm.parentEventID.qualifiedName(), "777");
      core2.put(DwcTerm.samplingProtocol.qualifiedName(), "samplingProtocol");
      core2.put(GbifTerm.eventType.qualifiedName(), "Find");

      Map<String, List<Map<String, String>>> ext = new HashMap<>();
      ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder()
              .setId(ID)
              .setCoreRowType(DwcTerm.Event.qualifiedName())
              .setCoreTerms(core)
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord);

      ExtendedRecord extendedRecord2 =
          ExtendedRecord.newBuilder()
              .setId(ID2)
              .setCoreRowType(DwcTerm.Event.qualifiedName())
              .setCoreTerms(core2)
              .build();
      writer.append(extendedRecord2);
    }
    Path from = Paths.get(outputFile, DATASET_KEY, attempt, "event/verbatim/interpret-777.avro");
    Path to = Paths.get(outputFile, DATASET_KEY, attempt, "verbatim.avro");
    Files.deleteIfExists(to);
    Files.move(from, to);

    // When
    VerbatimToEventPipeline.run(options, opt -> p);

    // Shoud
    String metricsOutput =
        String.join("/", outputFile, DATASET_KEY, attempt, "verbatim-to-occurrence.yml");
    assertTrue(Files.exists(Paths.get(metricsOutput)));

    String interpretedOutput = String.join("/", outputFile, DATASET_KEY, attempt, "event");

    assertEquals(11, new File(interpretedOutput).listFiles().length);
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier");
    assertFile(ExtendedRecord.class, interpretedOutput + "/verbatim");
    assertFile(EventCoreRecord.class, interpretedOutput + "/event");
    assertFile(MeasurementOrFactRecord.class, interpretedOutput + "/measurement_or_fact");
    assertEventCoreRecord(interpretedOutput + "/event");
  }

  private void assertEventCoreRecord(String output) throws Exception {

    Assert.assertTrue(Files.exists(Paths.get(output)));

    List<File> files =
        Arrays.stream(new File(output).listFiles())
            .filter(x -> x.toString().endsWith(".avro"))
            .collect(Collectors.toList());

    Assert.assertTrue(files.size() > 0);

    DatumReader<EventCoreRecord> ohrDatumReader = new SpecificDatumReader<>(EventCoreRecord.class);
    Map<String, EventCoreRecord> recordsMap = new HashMap<>();
    for (File file : files) {
      try (DataFileReader<EventCoreRecord> dataFileReader =
          new DataFileReader<>(file, ohrDatumReader)) {
        while (dataFileReader.hasNext()) {
          EventCoreRecord record = dataFileReader.next();
          Assert.assertNotNull(record);
          recordsMap.put(record.getId(), record);
        }
      }
    }

    EventCoreRecord record2 = recordsMap.get(ID2);
    Assert.assertEquals(1, record2.getParentsLineage().size());
    Assert.assertEquals(ID, record2.getParentsLineage().get(0).getId());
    Assert.assertEquals(1, record2.getParentsLineage().get(0).getOrder().intValue());
    Assert.assertNotNull(record2.getParentsLineage().get(0).getEventType());
  }

  private <T extends SpecificRecordBase> void assertFile(Class<T> clazz, String output)
      throws Exception {

    Assert.assertTrue(Files.exists(Paths.get(output)));

    List<File> files =
        Arrays.stream(new File(output).listFiles())
            .filter(x -> x.toString().endsWith(".avro"))
            .collect(Collectors.toList());

    Assert.assertTrue(files.size() > 0);

    for (File file : files) {
      Assert.assertTrue(file.exists());
      DatumReader<T> ohrDatumReader = new SpecificDatumReader<>(clazz);
      try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, ohrDatumReader)) {
        while (dataFileReader.hasNext()) {
          T record = dataFileReader.next();
          Assert.assertNotNull(record);

          String id = (String) record.get("id");
          if (record instanceof MetadataRecord) {
            Assert.assertEquals(DATASET_KEY, id);
          } else {
            Assert.assertTrue(id.equals(ID) || id.equals(ID2));
          }
        }
      }
    }
  }
}
