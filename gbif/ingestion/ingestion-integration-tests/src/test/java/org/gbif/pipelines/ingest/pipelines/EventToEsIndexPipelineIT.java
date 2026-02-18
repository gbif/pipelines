package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.core.converters.OccurrenceJsonConverter.GBIF_BACKBONE_DATASET_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.Response;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.factory.SerDeFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.resources.EsServer;
import org.gbif.pipelines.ingest.resources.ZkServer;
import org.gbif.pipelines.ingest.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.MultiTaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.DnaDerivedDataTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class EventToEsIndexPipelineIT {

  private static final DwcTerm EVENT_TERM = DwcTerm.Event;
  private static final DwcTerm OCCURRENCE_TERM = DwcTerm.Occurrence;

  private static final String ID = "777";
  private static final String SUB_EVENT_ID = "888";
  private static final String SUB_EVENT_ID_2 = "999";
  private static final String OCC_ID_1 = "occ1";
  private static final String OCC_ID_2 = "occ2";
  private static final String OCC_ID_3 = "occ3";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();

  private static ExtendedRecord testEventCoreRecord(String id, String parentId, String datasetKey) {
    Map<String, String> coreEvent1 = new HashMap<>();
    coreEvent1.put(DwcTerm.datasetID.qualifiedName(), datasetKey);
    coreEvent1.put(DwcTerm.institutionID.qualifiedName(), "institutionID");
    coreEvent1.put(DwcTerm.datasetName.qualifiedName(), "datasetName");
    coreEvent1.put(DwcTerm.eventID.qualifiedName(), ID);
    if (parentId == null) {
      coreEvent1.put(DwcTerm.parentEventID.qualifiedName(), parentId);
    }
    coreEvent1.put(DwcTerm.samplingProtocol.qualifiedName(), "samplingProtocol");
    coreEvent1.put(DwcTerm.fundingAttributionID.qualifiedName(), "FA1|FA2");

    return ExtendedRecord.newBuilder()
        .setId(id)
        .setCoreRowType(DwcTerm.Event.qualifiedName())
        .setCoreId(parentId)
        .setCoreTerms(coreEvent1)
        .build();
  }

  @Test
  public void indexingPipelineTest() throws Exception {

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
      "--metaFileName=occurrence-to-index.yml",
      "--inputPath=" + input + "/" + datasetKey + "/1/",
      "--targetPath=" + input,
      "--numberOfShards=1",
      "--interpretationTypes=ALL",
      "--dwcCore=Event"
    };
    InterpretationPipelineOptions optionsWriter =
        PipelinesOptionsFactory.createInterpretation(argsWriter);

    // Event
    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, VerbatimTransform.create(), EVENT_TERM, postfix)) {

      ExtendedRecord extendedRecord = testEventCoreRecord(ID, ID, datasetKey);
      writer.append(extendedRecord);

      ExtendedRecord subEventExtendedRecord = testEventCoreRecord(SUB_EVENT_ID, ID, datasetKey);
      writer.append(subEventExtendedRecord);

      ExtendedRecord subEventExtendedRecord2 =
          testEventCoreRecord(SUB_EVENT_ID_2, SUB_EVENT_ID, datasetKey);
      writer.append(subEventExtendedRecord2);
    }
    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, IdentifierTransform.builder().create(), EVENT_TERM, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId(ID).build();
      writer.append(identifierRecord);

      IdentifierRecord subEventIdentifierRecord =
          IdentifierRecord.newBuilder().setId(SUB_EVENT_ID).setInternalId(SUB_EVENT_ID).build();
      writer.append(subEventIdentifierRecord);

      IdentifierRecord subEventIdentifierRecord2 =
          IdentifierRecord.newBuilder().setId(SUB_EVENT_ID_2).setInternalId(SUB_EVENT_ID_2).build();
      writer.append(subEventIdentifierRecord2);
    }
    try (SyncDataFileWriter<EventCoreRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, EventCoreTransform.builder().create(), EVENT_TERM, postfix)) {
      EventCoreRecord eventCoreRecord =
          EventCoreRecord.newBuilder()
              .setId(ID)
              .setLocationID("L0")
              .setEventType(
                  VocabularyConcept.newBuilder()
                      .setConcept("Survey")
                      .setLineage(Collections.singletonList("Survey"))
                      .build())
              .setFundingAttributionID(Arrays.asList("FA1", "FA2"))
              .build();
      writer.append(eventCoreRecord);

      EventCoreRecord subEventCoreRecord =
          EventCoreRecord.newBuilder()
              .setId(SUB_EVENT_ID)
              .setEventType(
                  VocabularyConcept.newBuilder()
                      .setConcept("Survey")
                      .setLineage(Collections.singletonList("Survey"))
                      .build())
              .setParentEventID(ID)
              .setParentsLineage(
                  Collections.singletonList(Parent.newBuilder().setId(ID).setOrder(0).build()))
              .setLocationID("L1")
              .build();
      writer.append(subEventCoreRecord);

      EventCoreRecord subEventCoreRecord2 =
          EventCoreRecord.newBuilder()
              .setId(SUB_EVENT_ID_2)
              .setParentEventID(SUB_EVENT_ID)
              .setEventType(
                  VocabularyConcept.newBuilder()
                      .setConcept("Survey")
                      .setLineage(Collections.singletonList("Survey"))
                      .build())
              .setParentsLineage(
                  Arrays.asList(
                      Parent.newBuilder().setId(ID).setOrder(0).build(),
                      Parent.newBuilder()
                          .setId(SUB_EVENT_ID)
                          .setEventType("survey")
                          .setOrder(1)
                          .build()))
              .build();
      writer.append(subEventCoreRecord2);
    }

    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), EVENT_TERM, postfix)) {
      MetadataRecord metadataRecord =
          MetadataRecord.newBuilder().setId(ID).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), EVENT_TERM, postfix)) {
      TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
      writer.append(temporalRecord);

      TemporalRecord temporalRecordSubEvent =
          TemporalRecord.newBuilder()
              .setId(SUB_EVENT_ID)
              .setParentId(ID)
              .setEventDate(
                  EventDate.newBuilder().setGte("2017-10-10").setLte("2020-10-10").build())
              .setMonth(10)
              .setYear(2017)
              .build();
      writer.append(temporalRecordSubEvent);

      TemporalRecord temporalRecordSubEvent2 =
          TemporalRecord.newBuilder()
              .setId(SUB_EVENT_ID_2)
              .setParentId(SUB_EVENT_ID)
              .setEventDate(
                  EventDate.newBuilder().setGte("2019-10-10").setLte("2021-10-10").build())
              .build();
      writer.append(temporalRecordSubEvent2);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), EVENT_TERM, postfix)) {
      LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
      writer.append(locationRecord);

      LocationRecord locationRecordSubEvent =
          LocationRecord.newBuilder()
              .setId(SUB_EVENT_ID)
              .setParentId(ID)
              .setDecimalLatitude(10d)
              .setDecimalLongitude(5d)
              .setHasCoordinate(Boolean.TRUE)
              .setCountryCode("DK")
              .build();
      writer.append(locationRecordSubEvent);

      LocationRecord locationRecordSubEvent2 =
          LocationRecord.newBuilder().setId(SUB_EVENT_ID_2).setParentId(SUB_EVENT_ID).build();
      writer.append(locationRecordSubEvent2);
    }
    try (SyncDataFileWriter<MultimediaRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultimediaTransform.builder().create(), EVENT_TERM, postfix)) {
      MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
      writer.append(multimediaRecord);
    }
    try (SyncDataFileWriter<ImageRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ImageTransform.builder().create(), EVENT_TERM, postfix)) {
      ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
      writer.append(imageRecord);
    }
    try (SyncDataFileWriter<AudubonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, AudubonTransform.builder().create(), EVENT_TERM, postfix)) {
      AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
      writer.append(audubonRecord);
    }
    try (SyncDataFileWriter<DnaDerivedDataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, DnaDerivedDataTransform.builder().create(), EVENT_TERM, postfix)) {
      DnaDerivedDataRecord dnaRecord = DnaDerivedDataRecord.newBuilder().setId(ID).build();
      writer.append(dnaRecord);
    }

    // Occurrence
    optionsWriter.setDwcCore(DwcTerm.Occurrence);
    try (SyncDataFileWriter<ExtendedRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, VerbatimTransform.create(), OCCURRENCE_TERM, postfix)) {
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

      ExtendedRecord extendedRecord1 =
          ExtendedRecord.newBuilder()
              .setId(OCC_ID_1)
              .setCoreId(ID)
              .setCoreTerms(
                  Map.of(
                      DwcTerm.taxonID.qualifiedName(),
                      "taxonID1",
                      DwcTerm.eventID.qualifiedName(),
                      ID))
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord1);

      // Occurrence vinculada al primer sub-evento (SUB_EVENT_ID)
      ExtendedRecord extendedRecord2 =
          ExtendedRecord.newBuilder()
              .setId(OCC_ID_2)
              .setCoreId(ID)
              .setCoreTerms(
                  Map.of(
                      DwcTerm.taxonID.qualifiedName(),
                      "taxonID2",
                      DwcTerm.eventID.qualifiedName(),
                      SUB_EVENT_ID))
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord2);

      // Occurrence vinculada al segundo sub-evento (SUB_EVENT_ID_2)
      ExtendedRecord extendedRecord3 =
          ExtendedRecord.newBuilder()
              .setId(OCC_ID_3)
              .setCoreId(ID)
              .setCoreTerms(
                  Map.of(
                      DwcTerm.taxonID.qualifiedName(),
                      "taxonID3",
                      DwcTerm.eventID.qualifiedName(),
                      SUB_EVENT_ID_2))
              .setExtensions(ext)
              .build();
      writer.append(extendedRecord3);
    }

    Path occMetadataPath =
        Paths.get(
            PathBuilder.buildDatasetAttemptPath(
                optionsWriter, PipelinesVariables.Pipeline.VERBATIM_TO_OCCURRENCE + ".yml", false));
    Files.createFile(occMetadataPath);

    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GbifIdTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      IdentifierRecord identifierRecord1 =
          IdentifierRecord.newBuilder().setId(OCC_ID_1).setInternalId("1").build();
      writer.append(identifierRecord1);

      IdentifierRecord identifierRecord2 =
          IdentifierRecord.newBuilder().setId(OCC_ID_2).setInternalId("2").build();
      writer.append(identifierRecord2);

      IdentifierRecord identifierRecord3 =
          IdentifierRecord.newBuilder().setId(OCC_ID_3).setInternalId("3").build();
      writer.append(identifierRecord3);
    }
    try (SyncDataFileWriter<ClusteringRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ClusteringTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      ClusteringRecord clusteringRecord1 = ClusteringRecord.newBuilder().setId(OCC_ID_1).build();
      writer.append(clusteringRecord1);
      ClusteringRecord clusteringRecord2 = ClusteringRecord.newBuilder().setId(OCC_ID_2).build();
      writer.append(clusteringRecord2);
      ClusteringRecord clusteringRecord3 = ClusteringRecord.newBuilder().setId(OCC_ID_3).build();
      writer.append(clusteringRecord3);
    }
    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, BasicTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      BasicRecord basicRecord1 = BasicRecord.newBuilder().setId(OCC_ID_1).build();
      writer.append(basicRecord1);
      BasicRecord basicRecord2 = BasicRecord.newBuilder().setId(OCC_ID_2).build();
      writer.append(basicRecord2);
      BasicRecord basicRecord3 = BasicRecord.newBuilder().setId(OCC_ID_3).build();
      writer.append(basicRecord3);
    }
    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      MetadataRecord metadataRecord1 =
          MetadataRecord.newBuilder().setId(OCC_ID_1).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord1);
      MetadataRecord metadataRecord2 =
          MetadataRecord.newBuilder().setId(OCC_ID_2).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord2);
      MetadataRecord metadataRecord3 =
          MetadataRecord.newBuilder().setId(OCC_ID_3).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord3);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      TemporalRecord temporalRecord1 = TemporalRecord.newBuilder().setId(OCC_ID_1).build();
      writer.append(temporalRecord1);
      TemporalRecord temporalRecord2 = TemporalRecord.newBuilder().setId(OCC_ID_2).build();
      writer.append(temporalRecord2);
      TemporalRecord temporalRecord3 = TemporalRecord.newBuilder().setId(OCC_ID_3).build();
      writer.append(temporalRecord3);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      LocationRecord locationRecord1 =
          LocationRecord.newBuilder()
              .setId(OCC_ID_1)
              .setHasCoordinate(true)
              .setDecimalLatitude(10d)
              .setDecimalLongitude(5d)
              .build();
      writer.append(locationRecord1);
      LocationRecord locationRecord2 =
          LocationRecord.newBuilder()
              .setId(OCC_ID_2)
              .setHasCoordinate(true)
              .setDecimalLatitude(10d)
              .setDecimalLongitude(5d)
              .build();
      writer.append(locationRecord2);
      LocationRecord locationRecord3 =
          LocationRecord.newBuilder()
              .setId(OCC_ID_3)
              .setHasCoordinate(true)
              .setDecimalLatitude(10d)
              .setDecimalLongitude(5d)
              .build();
      writer.append(locationRecord3);
    }
    try (SyncDataFileWriter<MultiTaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultiTaxonomyTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      // Occurrence 1 - vinculada al evento raíz con 3 clasificaciones
      MultiTaxonRecord multiTaxonRecord1 =
          MultiTaxonRecord.newBuilder()
              .setId(OCC_ID_1)
              .setCoreId(ID)
              .setTaxonRecords(
                  Collections.singletonList(
                      TaxonRecord.newBuilder()
                          .setId("1")
                          .setClassification(
                              Arrays.asList(
                                  RankedName.newBuilder()
                                      .setRank(Rank.KINGDOM.toString())
                                      .setName("Animalia")
                                      .setKey("1")
                                      .build(),
                                  RankedName.newBuilder()
                                      .setRank(Rank.FAMILY.toString())
                                      .setName("Felidae")
                                      .setKey("9703")
                                      .build(),
                                  RankedName.newBuilder()
                                      .setRank(Rank.SPECIES.toString())
                                      .setName("Puma concolor")
                                      .setKey("2435099")
                                      .build()))
                          .setDatasetKey(GBIF_BACKBONE_DATASET_KEY)
                          .build()))
              .build();
      writer.append(multiTaxonRecord1);

      MultiTaxonRecord multiTaxonRecord2 =
          MultiTaxonRecord.newBuilder()
              .setId(OCC_ID_2)
              .setCoreId(ID)
              .setTaxonRecords(
                  Collections.singletonList(
                      TaxonRecord.newBuilder()
                          .setId("2")
                          .setClassification(
                              Arrays.asList(
                                  RankedName.newBuilder()
                                      .setRank(Rank.KINGDOM.toString())
                                      .setName("Plantae")
                                      .setKey("6")
                                      .build(),
                                  RankedName.newBuilder()
                                      .setRank(Rank.SPECIES.toString())
                                      .setName("Quercus robur")
                                      .setKey("2878688")
                                      .build()))
                          .setDatasetKey(GBIF_BACKBONE_DATASET_KEY)
                          .build()))
              .build();
      writer.append(multiTaxonRecord2);

      MultiTaxonRecord multiTaxonRecord3 =
          MultiTaxonRecord.newBuilder()
              .setId(OCC_ID_3)
              .setCoreId(ID)
              .setTaxonRecords(
                  Collections.singletonList(
                      TaxonRecord.newBuilder()
                          .setId("3")
                          .setClassification(
                              Collections.singletonList(
                                  RankedName.newBuilder()
                                      .setRank(Rank.SPECIES.toString())
                                      .setName("Puma concolor subsp. coryi")
                                      .setKey("6164600")
                                      .build()))
                          .setDatasetKey(GBIF_BACKBONE_DATASET_KEY)
                          .build()))
              .build();
      writer.append(multiTaxonRecord3);
    }
    try (SyncDataFileWriter<GrscicollRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GrscicollTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
      writer.append(grscicollRecord);
    }
    try (SyncDataFileWriter<MultimediaRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MultimediaTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
      writer.append(multimediaRecord);
    }
    try (SyncDataFileWriter<ImageRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ImageTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
      writer.append(imageRecord);
    }
    try (SyncDataFileWriter<AudubonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, AudubonTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
      writer.append(audubonRecord);
    }
    try (SyncDataFileWriter<DnaDerivedDataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, DnaDerivedDataTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      DnaDerivedDataRecord dnaDerivedDataRecord =
          DnaDerivedDataRecord.newBuilder().setId(ID).build();
      writer.append(dnaDerivedDataRecord);
    }

    // When
    String[] args = {
      "--datasetId=" + datasetKey,
      "--attempt=1",
      "--runner=SparkRunner",
      "--metaFileName=occurrence-to-index.yml",
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
    EventToEsIndexPipeline.run(options, opt -> p);

    EsService.refreshIndex(ES_SERVER.getEsClient(), idxName);

    // Should
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxName));
    assertEquals(3, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));
    assertResultsSize(idxName, "occurrence", 0);

    ParentJsonRecord eventRecord = getResult(idxName, ID, "event");
    assertRootParenJsonRecordResponse(eventRecord);

    ParentJsonRecord subEvent1 = getResult(idxName, SUB_EVENT_ID, "event");
    if (subEvent1.getDerivedMetadata().getTaxonomicCoverage() != null
        && subEvent1.getDerivedMetadata().getTaxonomicCoverage().getClassifications() != null) {
      Assert.assertEquals(
          2,
          subEvent1
              .getDerivedMetadata()
              .getTaxonomicCoverage()
              .getClassifications()
              .values()
              .iterator()
              .next()
              .size());
    }

    ParentJsonRecord subEvent2 = getResult(idxName, SUB_EVENT_ID_2, "event");
    if (subEvent2.getDerivedMetadata().getTaxonomicCoverage() != null
        && subEvent2.getDerivedMetadata().getTaxonomicCoverage().getClassifications() != null) {
      Assert.assertEquals(
          1,
          subEvent2
              .getDerivedMetadata()
              .getTaxonomicCoverage()
              .getClassifications()
              .values()
              .iterator()
              .next()
              .size());
    }

    ParentJsonRecord eventRecordSub2 = getResult(idxName, SUB_EVENT_ID_2, "event");
    assertEquals("DK", eventRecordSub2.getLocationInherited().getCountryCode());
    assertEquals(SUB_EVENT_ID_2, eventRecordSub2.getTemporalInherited().getId());
    assertEquals(Integer.valueOf(10), eventRecordSub2.getTemporalInherited().getMonth());
    assertEquals(Integer.valueOf(2017), eventRecordSub2.getTemporalInherited().getYear());
    assertEquals(
        Collections.singletonList("survey"), eventRecordSub2.getEventInherited().getEventType());
    assertEquals("L1", eventRecordSub2.getEventInherited().getLocationID());
  }

  @SneakyThrows
  private void assertResultsSize(String idxName, String type, int expectedSize) {
    Response response =
        EsService.executeQuery(
            ES_SERVER.getEsClient(),
            idxName,
            "{\n"
                + "  \"query\": {\n"
                + "    \"bool\" : {\n"
                + "      \"must\" : [\n"
                + "        {\n"
                + "          \"term\": {\n"
                + "            \"type\": \""
                + type
                + "\"\n"
                + "          }\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}");
    ObjectMapper mapper = SerDeFactory.avroMapperNonNulls();
    ArrayNode results =
        (ArrayNode)
            mapper
                .readValue(
                    IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8),
                    JsonNode.class)
                .get("hits")
                .get("hits");
    assertEquals(expectedSize, results.size());
  }

  /**
   * Executes an Elasticsearch query, retrieves the first result and returns it as ParentJsonRecord.
   */
  @SneakyThrows
  private ParentJsonRecord getResult(String idxName, String id, String type) {
    Response response =
        EsService.executeQuery(
            ES_SERVER.getEsClient(),
            idxName,
            "{\n"
                + "  \"query\": {\n"
                + "    \"bool\" : {\n"
                + "      \"must\" : [\n"
                + "        {\n"
                + "          \"term\": {\n"
                + "            \"id\": \""
                + id
                + "\"\n"
                + "          }\n"
                + "        },\n"
                + "        {\n"
                + "          \"term\": {\n"
                + "            \"type\": \""
                + type
                + "\"\n"
                + "          }\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }\n"
                + "}");
    ObjectMapper mapper = SerDeFactory.avroMapperNonNulls();
    ArrayNode results =
        (ArrayNode)
            mapper
                .readValue(
                    IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8),
                    JsonNode.class)
                .get("hits")
                .get("hits");
    return mapper.treeToValue(results.get(0).get("_source"), ParentJsonRecord.class);
  }

  /** Asserts the Parent Event Record by checking the expected nested data. */
  private void assertRootParenJsonRecordResponse(ParentJsonRecord record) {
    Assert.assertNotNull(record.getInternalId());

    // Assert temporal coverage
    Assert.assertNotNull(record.getDerivedMetadata().getTemporalCoverage());
    Assert.assertEquals("2017-10-10", record.getDerivedMetadata().getTemporalCoverage().getGte());
    Assert.assertEquals("2021-10-10", record.getDerivedMetadata().getTemporalCoverage().getLte());

    // Assert geographic coverage/convex hull
    Assert.assertNotNull(record.getDerivedMetadata().getWktConvexHull());
    Assert.assertEquals("POINT(5.0 10.0)", record.getDerivedMetadata().getWktConvexHull());

    // Assert taxonomic coverage
    Assert.assertNotNull(record.getDerivedMetadata().getTaxonomicCoverage());
    Assert.assertEquals(
        1, record.getDerivedMetadata().getTaxonomicCoverage().getClassifications().size());
    Assert.assertEquals(
        GBIF_BACKBONE_DATASET_KEY,
        record
            .getDerivedMetadata()
            .getTaxonomicCoverage()
            .getClassifications()
            .keySet()
            .iterator()
            .next());
    Assert.assertNotNull(record.getDerivedMetadata().getTaxonomicCoverage().getTaxonIDs());
    Assert.assertEquals(3, record.getDerivedMetadata().getTaxonomicCoverage().getTaxonIDs().size());
    Assert.assertTrue(
        record.getDerivedMetadata().getTaxonomicCoverage().getTaxonIDs().contains("taxonID1"));
    Assert.assertEquals(
        3,
        record
            .getDerivedMetadata()
            .getTaxonomicCoverage()
            .getClassifications()
            .values()
            .iterator()
            .next()
            .size());

    Assert.assertEquals(2, record.getEvent().getFundingAttributionID().size());
  }
}
