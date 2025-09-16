package org.gbif.pipelines.ingest.pipelines;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.api.model.pipelines.InterpretationType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.utils.HdfsViewUtils;
import org.gbif.pipelines.ingest.resources.ZkServer;
import org.gbif.pipelines.ingest.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MultiTaxonomyTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.DnaDerivedDataTransform;
import org.gbif.pipelines.transforms.extension.HumboldtTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class HdfsViewPipelineIT {

  private static final String ID = "777";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();

  @Test
  public void pipelineAllTest() throws Exception {
    DwcTerm coreTerm = DwcTerm.Occurrence;

    // State
    String outputFile = getClass().getResource("/").getFile();

    String datasetKey = UUID.randomUUID().toString();
    String postfix = "777";

    String input = outputFile + "data0/ingest";
    String output = outputFile + "data0/hdfsview";

    String[] argsWriter = {
      "--datasetId=" + datasetKey,
      "--attempt=147",
      "--runner=SparkRunner",
      "--metaFileName=occurrence-to-hdfs.yml",
      "--inputPath=" + output,
      "--targetPath=" + input,
      "--numberOfShards=1",
      "--interpretationTypes=OCCURRENCE,MEASUREMENT_OR_FACT_TABLE,EXTENDED_MEASUREMENT_OR_FACT_TABLE,GERMPLASM_MEASUREMENT_TRIAL_TABLE,AUDUBON_TABLE,HUMBOLDT_TABLE",
      "--properties=" + outputFile + "/data7/ingest/pipelines.yaml"
    };
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(argsWriter);

    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, VerbatimTransform.create(), coreTerm, postfix)) {
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
          ExtendedRecord.newBuilder().setId(ID).setExtensions(ext).build();
      writer.append(extendedRecord);
    }
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GbifIdTransform.builder().create(), coreTerm, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
      writer.append(identifierRecord);
    }
    try (SyncDataFileWriter<ClusteringRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ClusteringTransform.builder().create(), coreTerm, postfix)) {
      ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
      writer.append(clusteringRecord);
    }
    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, BasicTransform.builder().create(), coreTerm, postfix)) {
      BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
      writer.append(basicRecord);
    }
    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), coreTerm, postfix)) {
      MetadataRecord metadataRecord =
          MetadataRecord.newBuilder().setId(ID).setDatasetKey("dataset_key").build();
      writer.append(metadataRecord);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), coreTerm, postfix)) {
      TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
      writer.append(temporalRecord);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), coreTerm, postfix)) {
      LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
      writer.append(locationRecord);
    }
    try (SyncDataFileWriter<TaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TaxonomyTransform.builder().create(), coreTerm, postfix)) {
      TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
      writer.append(taxonRecord);
    }
    try (SyncDataFileWriter<MultiTaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultiTaxonomyTransform.builder().create(), coreTerm, postfix)) {
      MultiTaxonRecord multiTaxonRecord = MultiTaxonRecord.newBuilder().setId(ID).build();
      writer.append(multiTaxonRecord);
    }
    try (SyncDataFileWriter<GrscicollRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GrscicollTransform.builder().create(), coreTerm, postfix)) {
      GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
      writer.append(grscicollRecord);
    }
    try (SyncDataFileWriter<MultimediaRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultimediaTransform.builder().create(), coreTerm, postfix)) {
      MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
      writer.append(multimediaRecord);
    }
    try (SyncDataFileWriter<ImageRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ImageTransform.builder().create(), coreTerm, postfix)) {
      ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
      writer.append(imageRecord);
    }
    try (SyncDataFileWriter<DnaDerivedDataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, DnaDerivedDataTransform.builder().create(), coreTerm, postfix)) {
      DnaDerivedDataRecord dnaRecord = DnaDerivedDataRecord.newBuilder().setId(ID).build();
      writer.append(dnaRecord);
    }
    try (SyncDataFileWriter<AudubonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, AudubonTransform.builder().create(), coreTerm, postfix)) {
      AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
      writer.append(audubonRecord);
    }
    try (SyncDataFileWriter<HumboldtRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, HumboldtTransform.builder().create(), coreTerm, postfix)) {
      HumboldtRecord humboldtRecord = HumboldtRecord.newBuilder().setId(ID).build();
      writer.append(humboldtRecord);
    }

    // When
    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=147",
      "--runner=SparkRunner",
      "--metaFileName=occurrence-to-hdfs.yml",
      "--inputPath=" + input,
      "--targetPath=" + output,
      "--numberOfShards=1",
      "--interpretationTypes=OCCURRENCE,MEASUREMENT_OR_FACT_TABLE,EXTENDED_MEASUREMENT_OR_FACT_TABLE,GERMPLASM_MEASUREMENT_TRIAL_TABLE,AUDUBON_TABLE",
      "--testMode=true"
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    HdfsViewPipeline.run(options, opt -> p);

    Function<String, String> outputFn =
        s -> output + "/occurrence/" + s + "/" + datasetKey + "_147-00000-of-00001.avro";

    assertFile(OccurrenceHdfsRecord.class, outputFn.apply("occurrence"));
    assertFile(MeasurementOrFactTable.class, outputFn.apply("measurementorfacttable"));

    assertFileExistFalse(outputFn.apply("germplasmmeasurementtrialtable"));
    assertFileExistFalse(outputFn.apply("audubontable"));
    assertFileExistFalse(outputFn.apply("extendedmeasurementorfacttable"));
    assertFileExistFalse(outputFn.apply("permittable"));
    assertFileExistFalse(outputFn.apply("loantable"));
  }

  @Test
  public void pipelineOccurrenceTest() throws Exception {
    pipelineHDFSTest(InterpretationType.RecordType.OCCURRENCE);
  }

  @Test
  public void pipelineEventTest() throws Exception {
    pipelineHDFSTest(InterpretationType.RecordType.EVENT);
  }

  public void pipelineHDFSTest(InterpretationType.RecordType recordType) throws Exception {

    DwcTerm coreTerm = HdfsViewUtils.getCoreTerm(recordType);

    // State
    String outputFile = getClass().getResource("/").getFile();

    String datasetKey = UUID.randomUUID().toString();
    String postfix = "777";

    String input = outputFile + "data1/ingest";
    String output = outputFile + "data1/hdfsview";

    String[] argsWriter = {
      "--datasetId=" + datasetKey,
      "--attempt=147",
      "--runner=SparkRunner",
      "--metaFileName=occurrence-to-hdfs.yml",
      "--inputPath=" + output,
      "--targetPath=" + input,
      "--numberOfShards=1",
      "--interpretationTypes=" + recordType.name()
    };
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(argsWriter);

    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, VerbatimTransform.create(), coreTerm, postfix)) {
      Map<String, String> ext1 = new HashMap<>();
      ext1.put(DwcTerm.eventID.qualifiedName(), "Id1");
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
              .setCoreRowType(coreTerm.qualifiedName())
              .setId(ID)
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord);
    }
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GbifIdTransform.builder().create(), coreTerm, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
      writer.append(identifierRecord);
    }
    try (SyncDataFileWriter<ClusteringRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ClusteringTransform.builder().create(), coreTerm, postfix)) {
      ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
      writer.append(clusteringRecord);
    }
    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, BasicTransform.builder().create(), coreTerm, postfix)) {
      BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
      writer.append(basicRecord);
    }
    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), coreTerm, postfix)) {
      MetadataRecord metadataRecord = MetadataRecord.newBuilder().setId(ID).build();
      writer.append(metadataRecord);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), coreTerm, postfix)) {
      TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
      writer.append(temporalRecord);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), coreTerm, postfix)) {
      LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
      writer.append(locationRecord);
    }
    try (SyncDataFileWriter<TaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TaxonomyTransform.builder().create(), coreTerm, postfix)) {
      TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
      writer.append(taxonRecord);
    }
    try (SyncDataFileWriter<MultiTaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultiTaxonomyTransform.builder().create(), coreTerm, postfix)) {
      MultiTaxonRecord multiTaxonRecord = MultiTaxonRecord.newBuilder().setId(ID).build();
      writer.append(multiTaxonRecord);
    }
    try (SyncDataFileWriter<GrscicollRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GrscicollTransform.builder().create(), coreTerm, postfix)) {
      GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
      writer.append(grscicollRecord);
    }
    try (SyncDataFileWriter<MultimediaRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultimediaTransform.builder().create(), coreTerm, postfix)) {
      MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
      writer.append(multimediaRecord);
    }
    try (SyncDataFileWriter<ImageRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ImageTransform.builder().create(), coreTerm, postfix)) {
      ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
      writer.append(imageRecord);
    }
    try (SyncDataFileWriter<DnaDerivedDataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, DnaDerivedDataTransform.builder().create(), coreTerm, postfix)) {
      DnaDerivedDataRecord dnaRecord = DnaDerivedDataRecord.newBuilder().setId(ID).build();
      writer.append(dnaRecord);
    }
    try (SyncDataFileWriter<AudubonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, AudubonTransform.builder().create(), coreTerm, postfix)) {
      AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
      writer.append(audubonRecord);
    }
    try (SyncDataFileWriter<EventCoreRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, EventCoreTransform.builder().create(), coreTerm, postfix)) {
      EventCoreRecord eventCoreRecord = EventCoreRecord.newBuilder().setId(ID).build();
      writer.append(eventCoreRecord);
    }
    if (recordType == InterpretationType.RecordType.EVENT) {
      try (SyncDataFileWriter<HumboldtRecord> writer =
          InterpretedAvroWriter.createAvroWriter(
              optionsWriter, HumboldtTransform.builder().create(), coreTerm, postfix)) {
        HumboldtRecord humboldtRecord = HumboldtRecord.newBuilder().setId(ID).build();
        writer.append(humboldtRecord);
      }
    }

    // When
    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=147",
      "--runner=SparkRunner",
      "--metaFileName=occurrence-to-hdfs.yml",
      "--inputPath=" + input,
      "--targetPath=" + output,
      "--numberOfShards=1",
      "--interpretationTypes=" + recordType.name(),
      "--testMode=true"
    };

    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    options.setCoreRecordType(recordType);

    HdfsViewPipeline.run(options, opt -> p);

    Function<String, String> outputFn =
        s ->
            output
                + "/"
                + recordType.name().toLowerCase()
                + "/"
                + s
                + "/"
                + datasetKey
                + "_147-00000-of-00001.avro";

    assertFile(OccurrenceHdfsRecord.class, outputFn.apply(recordType.name().toLowerCase()));
    assertFileExistFalse(outputFn.apply("measurementorfacttable"));
    assertFileExistFalse(outputFn.apply("extendedmeasurementorfacttable"));
    assertFileExistFalse(outputFn.apply("germplasmmeasurementtrialtable"));
    assertFileExistFalse(outputFn.apply("permittable"));
    assertFileExistFalse(outputFn.apply("loantable"));

    if (recordType == InterpretationType.RecordType.EVENT) {
      assertFileExistFalse(outputFn.apply("humboldttable"));
    }
  }

  private void assertFileExistFalse(String output) {
    Assert.assertFalse(new File(output).exists());
  }

  private <T extends SpecificRecordBase> void assertFile(Class<T> clazz, String output)
      throws Exception {
    File file = new File(output);
    DatumReader<T> ohrDatumReader = new SpecificDatumReader<>(clazz);
    try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, ohrDatumReader)) {
      while (dataFileReader.hasNext()) {
        T record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals("1", record.get("gbifid"));
      }
    }
  }
}
