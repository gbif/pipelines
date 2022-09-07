package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;
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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("all")
public class VerbatimToOccurrencePipelineIT {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  private static final String ID = "777";
  private static final String DATASET_KEY = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";

  @Test
  public void pipelineAllSynchTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "55";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-occurrence.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--interpretationTypes=" + ALL,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--testMode=true"
    };

    // When, Should
    pipelineTest(args, attempt, outputFile);
  }

  @Test
  public void pipelineTaxonomySynchTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "77";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-occurrence.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--interpretationTypes=" + TAXONOMY,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--testMode=true"
    };

    String postfix = "777";
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(args);
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GbifIdTransform.builder().create(), CORE_TERM, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
      writer.append(identifierRecord);
    }

    // When, Should
    pipelineTest(args, attempt, outputFile);
  }

  @Test
  public void pipelineAllAsynchTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "71";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-occurrence.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--interpretationTypes=" + ALL,
      "--properties=" + outputFile + "/pipelines.yaml",
      "--syncThreshold=0",
      "--testMode=true"
    };

    // When, Should
    pipelineTest(args, attempt, outputFile);
  }

  @Test
  public void pipelineManySynchTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/data7/ingest").getFile();

    String attempt = "99";

    String[] args = {
      "--datasetId=" + DATASET_KEY,
      "--attempt=" + attempt,
      "--runner=SparkRunner",
      "--metaFileName=verbatim-to-occurrence.yml",
      "--inputPath=" + outputFile + "/" + DATASET_KEY + "/" + attempt + "/verbatim.avro",
      "--targetPath=" + outputFile,
      "--interpretationTypes=IDENTIFIER_ABSENT,CLUSTERING,TEMPORAL,LOCATION,GRSCICOLL,MULTIMEDIA,MEASUREMENT_OR_FACT_TABLE,BASIC,TAXONOMY,IMAGE,AMPLIFICATION,OCCURRENCE,VERBATIM,LOCATION_FEATURE,MEASUREMENT_OR_FACT,AUDUBON,METADATA",
      "--properties=" + outputFile + "/pipelines.yaml",
      "--testMode=true"
    };

    String postfix = "777";
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(args);
    GbifIdTransform transform = GbifIdTransform.builder().create();
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(optionsWriter, transform, CORE_TERM, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
      writer.append(identifierRecord);
    }
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, transform, CORE_TERM, postfix, transform.getAbsentName())) {
      IdentifierRecord identifierRecord = IdentifierRecord.newBuilder().setId(ID).build();
      writer.append(identifierRecord);
    }

    // When, Should
    pipelineTest(args, attempt, outputFile);
  }

  private void pipelineTest(String[] args, String attempt, String outputFile) throws Exception {

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

      Map<String, List<Map<String, String>>> ext = new HashMap<>();
      ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder()
              .setId(ID)
              .setCoreTerms(Collections.singletonMap("Key", "Value"))
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord);
    }
    Path from =
        Paths.get(outputFile, DATASET_KEY, attempt, "occurrence/verbatim/interpret-777.avro");
    Path to = Paths.get(outputFile, DATASET_KEY, attempt, "verbatim.avro");
    Files.deleteIfExists(to);
    Files.move(from, to);

    // When
    VerbatimToOccurrencePipeline.run(options);

    // Shoud
    String metricsOutput =
        String.join("/", outputFile, DATASET_KEY, attempt, "verbatim-to-occurrence.yml");
    assertTrue(Files.exists(Paths.get(metricsOutput)));

    String interpretedOutput = String.join("/", outputFile, DATASET_KEY, attempt, "occurrence");

    assertFile(AudubonRecord.class, interpretedOutput + "/audubon");
    assertFile(BasicRecord.class, interpretedOutput + "/basic");
    assertFile(ClusteringRecord.class, interpretedOutput + "/clustering");
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier");
    assertFile(IdentifierRecord.class, interpretedOutput + "/identifier_invalid");
    assertFile(GrscicollRecord.class, interpretedOutput + "/grscicoll");
    assertFile(ImageRecord.class, interpretedOutput + "/image");
    assertFile(LocationRecord.class, interpretedOutput + "/location");
    assertFile(MultimediaRecord.class, interpretedOutput + "/multimedia");
    assertFile(TaxonRecord.class, interpretedOutput + "/taxonomy");
    assertFile(TemporalRecord.class, interpretedOutput + "/temporal");
    assertFile(ExtendedRecord.class, interpretedOutput + "/verbatim");
    int expected = 12;
    if (options.getInterpretationTypes().contains(METADATA.name())
        || options.getInterpretationTypes().contains(ALL.name())) {
      assertFile(MetadataRecord.class, interpretedOutput + "/metadata");
      expected++;
    }
    assertEquals(expected, new File(interpretedOutput).listFiles().length);
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
        if (record instanceof MetadataRecord) {
          Assert.assertEquals(DATASET_KEY, id);
        } else {
          Assert.assertEquals(ID, id);
        }
      }
    }
  }
}
