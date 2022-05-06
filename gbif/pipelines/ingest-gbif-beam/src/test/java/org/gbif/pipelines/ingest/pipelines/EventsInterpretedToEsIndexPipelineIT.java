package org.gbif.pipelines.ingest.pipelines;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.pipelines.utils.EsServer;
import org.gbif.pipelines.ingest.pipelines.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;
import org.gbif.pipelines.transforms.converters.OccurrenceJsonTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class EventsInterpretedToEsIndexPipelineIT {

  private static final String ID = "777";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void interpretationPipelineTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/").getFile();
    String idxName = "eventinterpretedtoesindexextendedpipelineit";
    String postfix = "777";
    String input = outputFile + "data3";
    String datasetKey = UUID.randomUUID().toString();

    String[] argsWriter = {
      "--datasetId=" + datasetKey,
      "--attempt=1",
      "--runner=SparkRunner",
      "--metaFileName=interpreted-to-index.yml",
      "--inputPath=" + input + "/" + datasetKey + "/1/",
      "--targetPath=" + input,
      "--numberOfShards=1",
      "--interpretationTypes=ALL",
      "--dwcCore=Event"
    };
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(argsWriter);

    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, VerbatimTransform.create(), postfix)) {

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
              .setParentCoreId(ID)
              .setCoreTerms(core)
              .build();

      writer.append(extendedRecord);
    }
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, IdentifierTransform.builder().create(), postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId(ID).build();
      writer.append(identifierRecord);
    }
    try (SyncDataFileWriter<EventCoreRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, EventCoreTransform.builder().create(), postfix)) {
      EventCoreRecord eventCoreRecord = EventCoreRecord.newBuilder().setId(ID).build();
      writer.append(eventCoreRecord);
    }
    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), postfix)) {
      MetadataRecord metadataRecord =
          MetadataRecord.newBuilder().setId(ID).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), postfix)) {
      TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
      writer.append(temporalRecord);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), postfix)) {
      LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
      writer.append(locationRecord);
    }
    try (SyncDataFileWriter<MultimediaRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultimediaTransform.builder().create(), postfix)) {
      MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
      writer.append(multimediaRecord);
    }
    try (SyncDataFileWriter<ImageRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ImageTransform.builder().create(), postfix)) {
      ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
      writer.append(imageRecord);
    }
    try (SyncDataFileWriter<AudubonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, AudubonTransform.builder().create(), postfix)) {
      AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
      writer.append(audubonRecord);
    }

    optionsWriter.setDwcCore(DwcTerm.Occurrence);
    try (SyncDataFileWriter<OccurrenceJsonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter,
            OccurrenceJsonTransform.getBaseName(),
            OccurrenceJsonTransform.getAvroSchema(),
            postfix)) {
      OccurrenceJsonRecord occurrenceJsonRecord =
          OccurrenceJsonRecord.newBuilder()
              .setGbifId(Long.parseLong(ID))
              .setId(ID)
              .setDatasetKey(datasetKey)
              .setVerbatim(VerbatimRecord.newBuilder().setParentCoreId(ID).build())
              .build();
      writer.append(occurrenceJsonRecord);
    }

    // When
    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=1",
      "--runner=TestSparkRunner",
      "--metaFileName=interpreted-to-index.yml",
      "--inputPath=" + input,
      "--targetPath=" + input,
      "--esHosts=" + String.join(",", ES_SERVER.getEsConfig().getRawHosts()),
      "--esIndexName=eventinterpretedtoesindexextendedpipelineit",
      "--esAlias=event_interpretedtoesindexextendedpipelineit",
      "--indexNumberShards=1",
      "--indexNumberReplicas=0",
      "--esSchemaPath=elasticsearch/es-event-core-schema.json",
      "--esDocumentId=internalId",
      "--dwcCore=Event"
    };
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(args);
    EventsInterpretedToIndexPipeline.run(options, opt -> p);

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(2, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
  }
}
