package org.gbif.pipelines.ingest.pipelines;

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
import org.gbif.pipelines.ingest.pipelines.utils.EsServer;
import org.gbif.pipelines.ingest.pipelines.utils.InterpretedAvroWriter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
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
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.junit.Assert;
import org.junit.Before;
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

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

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
          EventCoreRecord.newBuilder().setId(ID).setLocationID("L0").build();
      writer.append(eventCoreRecord);

      EventCoreRecord subEventCoreRecord =
          EventCoreRecord.newBuilder()
              .setId(SUB_EVENT_ID)
              .setEventType(
                  VocabularyConcept.newBuilder()
                      .setConcept("survey")
                      .setLineage(Collections.emptyList())
                      .build())
              .setParentEventID(ID)
              .setParentsLineage(Collections.singletonList(Parent.newBuilder().setId(ID).build()))
              .setLocationID("L1")
              .build();
      writer.append(subEventCoreRecord);

      EventCoreRecord subEventCoreRecord2 =
          EventCoreRecord.newBuilder()
              .setId(SUB_EVENT_ID_2)
              .setParentEventID(SUB_EVENT_ID)
              .setParentsLineage(
                  Arrays.asList(
                      Parent.newBuilder().setId(ID).build(),
                      Parent.newBuilder().setId(SUB_EVENT_ID).setEventType("survey").build()))
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
    try (SyncDataFileWriter<TaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TaxonomyTransform.builder().create(), EVENT_TERM, postfix)) {
      TaxonRecord taxonRecord =
          TaxonRecord.newBuilder()
              .setId(ID)
              .setClassification(
                  Collections.singletonList(
                      RankedName.newBuilder()
                          .setRank(Rank.SPECIES)
                          .setName("Puma concolor subsp. coryi (Bangs, 1899)")
                          .setKey(6164600)
                          .build()))
              .build();
      writer.append(taxonRecord);

      TaxonRecord taxonRecordSubEvent =
          TaxonRecord.newBuilder()
              .setId(SUB_EVENT_ID)
              .setParentId(ID)
              .setClassification(
                  Collections.singletonList(
                      RankedName.newBuilder()
                          .setRank(Rank.SPECIES)
                          .setName("Puma concolor subsp. concolor")
                          .setKey(7193927)
                          .build()))
              .build();
      writer.append(taxonRecordSubEvent);

      TaxonRecord taxonRecordSubEvent2 =
          TaxonRecord.newBuilder()
              .setId(SUB_EVENT_ID_2)
              .setParentId(SUB_EVENT_ID)
              .setClassification(
                  Collections.singletonList(
                      RankedName.newBuilder()
                          .setRank(Rank.SPECIES)
                          .setName("Puma concolor (Linnaeus, 1771)")
                          .setKey(2435099)
                          .build()))
              .build();
      writer.append(taxonRecordSubEvent2);
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
    try (SyncDataFileWriter<MeasurementOrFactRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MeasurementOrFactTransform.builder().create(), EVENT_TERM, postfix)) {
      MeasurementOrFactRecord mofRecord = MeasurementOrFactRecord.newBuilder().setId(ID).build();
      writer.append(mofRecord);
    }

    optionsWriter.setDwcCore(DwcTerm.Occurrence);

    // Occurrence
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

      ExtendedRecord extendedRecord =
          ExtendedRecord.newBuilder().setId(ID).setCoreId(ID).setExtensions(ext).build();
      writer.append(extendedRecord);

      ExtendedRecord subEventExtendedRecord =
          ExtendedRecord.newBuilder().setId(SUB_EVENT_ID).setCoreId(ID).setExtensions(ext).build();
      writer.append(subEventExtendedRecord);
    }

    Path occMetadataPath =
        Paths.get(
            PathBuilder.buildDatasetAttemptPath(
                optionsWriter, PipelinesVariables.Pipeline.VERBATIM_TO_OCCURRENCE + ".yml", false));
    Files.createFile(occMetadataPath);

    try (SyncDataFileWriter<IdentifierRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, GbifIdTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      IdentifierRecord identifierRecord =
          IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
      writer.append(identifierRecord);

      IdentifierRecord subEventGbifIdRecord =
          IdentifierRecord.newBuilder().setId(SUB_EVENT_ID).setInternalId("2").build();
      writer.append(subEventGbifIdRecord);
    }
    try (SyncDataFileWriter<ClusteringRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, ClusteringTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
      writer.append(clusteringRecord);
    }
    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, BasicTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
      writer.append(basicRecord);
    }
    try (SyncDataFileWriter<MetadataRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, MetadataTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      MetadataRecord metadataRecord =
          MetadataRecord.newBuilder().setId(ID).setDatasetKey(datasetKey).build();
      writer.append(metadataRecord);
    }
    try (SyncDataFileWriter<TemporalRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TemporalTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
      writer.append(temporalRecord);
    }
    try (SyncDataFileWriter<LocationRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, LocationTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
      writer.append(locationRecord);
    }
    try (SyncDataFileWriter<TaxonRecord> writer =
        InterpretedAvroWriter.createAvroWriter(
            optionsWriter, TaxonomyTransform.builder().create(), OCCURRENCE_TERM, postfix)) {
      TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
      writer.append(taxonRecord);
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
    assertEquals(4, EsService.countIndexDocuments(ES_SERVER.getEsClient(), idxName));

    ParentJsonRecord eventRecord = getResult(idxName, ID, "event");
    assertRootParenJsonRecordResponse(eventRecord);

    ParentJsonRecord eventRecordSub2 = getResult(idxName, SUB_EVENT_ID_2, "event");
    assertEquals("DK", eventRecordSub2.getLocationInherited().getCountryCode());
    assertEquals(SUB_EVENT_ID_2, eventRecordSub2.getTemporalInherited().getId());
    assertEquals(new Integer(10), eventRecordSub2.getTemporalInherited().getMonth());
    assertEquals(new Integer(2017), eventRecordSub2.getTemporalInherited().getYear());
    assertEquals(
        Collections.singletonList("survey"), eventRecordSub2.getEventInherited().getEventType());
    assertEquals("L1", eventRecordSub2.getEventInherited().getLocationID());
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
    Assert.assertEquals("POINT (5 10)", record.getDerivedMetadata().getWktConvexHull());

    // Assert taxonomic coverage
    Assert.assertNotNull(record.getDerivedMetadata().getTaxonomicCoverage());
    Assert.assertEquals(2, record.getDerivedMetadata().getTaxonomicCoverage().size());
  }
}
