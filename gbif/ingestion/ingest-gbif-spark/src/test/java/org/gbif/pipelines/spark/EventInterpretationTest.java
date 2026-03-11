package org.gbif.pipelines.spark;

import static org.junit.Assert.*;

import com.jayway.jsonpath.JsonPath;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class EventInterpretationTest extends MockedServicesTest {

  @Test
  public void test() throws Exception {

    URL testRootUrl = getClass().getResource("/");
    assert testRootUrl != null;
    String testResourcesRoot = testRootUrl.getFile();

    String testUuid = "8d5fe649-f85e-43cc-a19c-2a9979a741ac";
    generateVerbatimAvro(testUuid, 1);

    EventInterpretation.main(
        new String[] {
          "--appName=event-interpretation-test",
          "--datasetId=" + testUuid,
          "--attempt=" + 1,
          "--config=" + testResourcesRoot + "/pipelines.yaml",
          "--master=local[*]",
          "--numberOfShards=1",
          "--useSystemExit=false"
        });

    // Validate EventHdfsRecord output
    checkHdfsTableOutputs(testResourcesRoot, testUuid, 1);
    checkJsonOutputs(testResourcesRoot, testUuid, 1);
  }

  private void checkHdfsTableOutputs(String outputFile, String testUuid, int attempt)
      throws Exception {

    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve(Directories.EVENT_HDFS);

    java.nio.file.Path parquetFile =
        Files.list(dir)
            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
            .findFirst()
            .get();

    Configuration conf = new Configuration();

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(new Path(parquetFile.toString()))
            .withConf(conf)
            .build()) {

      GenericRecord record;
      while ((record = reader.read()) != null) {

        // convert to JSON
        String json = toJson(record);
        String verbatimEventID = JsonPath.read(json, "$.VEventid.string");

        if ("EVT-000".equals(verbatimEventID)) {
          // Parent record checks
          assertEquals("EVT-000", JsonPath.read(json, "$.VEventid.string"));
          assertEquals("Project", JsonPath.read(json, "$.VEventtype.string"));
        } else if ("EVT-001".equals(verbatimEventID)) {
          // Child record checks
          assertEquals("EVT-001", JsonPath.read(json, "$.VEventid.string"));
          assertEquals("Survey", JsonPath.read(json, "$.VEventtype.string"));
          assertEquals("EVT-000", JsonPath.read(json, "$.VParenteventid.string"));
          assertEquals("2024-06-15", JsonPath.read(json, "$.VEventdate.string"));
          assertEquals("10:30:00Z", JsonPath.read(json, "$.VEventtime.string"));
          assertEquals("visual survey", JsonPath.read(json, "$.VSamplingprotocol.string"));
          assertEquals("2 hours", JsonPath.read(json, "$.VSamplingeffort.string"));
          assertEquals("Dummy event for testing", JsonPath.read(json, "$.VEventremarks.string"));
          assertEquals("Kenya", JsonPath.read(json, "$.VCountry.string"));
          assertEquals("KE", JsonPath.read(json, "$.countrycode.string"));
          assertEquals("Narok", JsonPath.read(json, "$.stateprovince.string"));
          assertEquals("Maasai Mara National Reserve", JsonPath.read(json, "$.VLocality.string"));
          assertEquals(-1.4061d, JsonPath.<Double>read(json, "$.decimallatitude.double"), 0.0001);
          assertEquals(35.0128d, JsonPath.<Double>read(json, "$.decimallongitude.double"), 0.0001);
        } else {
          throw new IllegalStateException("Unexpected verbatimEventID: " + verbatimEventID);
        }
      }
    }
  }

  private void checkJsonOutputs(String outputFile, String testUuid, int attempt) throws Exception {

    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve(Directories.EVENT_JSON);

    java.nio.file.Path parquetFile =
        Files.list(dir)
            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
            .findFirst()
            .get();

    Configuration conf = new Configuration();

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(new Path(parquetFile.toString()))
            .withConf(conf)
            .build()) {

      GenericRecord record;
      while ((record = reader.read()) != null) {

        // convert to JSON
        String json = toJson(record);

        // try to read event id from known locations (parent/child JSON shapes)
        String eventId = null;
        try {
          eventId = JsonPath.read(json, "$.event.event.eventID.string");
        } catch (Exception ignored) {
        }

        org.junit.Assert.assertNotNull("eventId must be present in JSON: " + json, eventId);

        if ("EVT-000".equals(eventId)) {
          // Top-level and derived metadata
          assertEquals("1", JsonPath.read(json, "$.id.string"));
          assertEquals(1, JsonPath.<Integer>read(json, "$.crawlId.int").intValue());
          assertEquals("1", JsonPath.read(json, "$.derivedMetadata.derivedMetadata.id.string"));

          // Event-level parent checks
          assertEquals("EVT-000", JsonPath.read(json, "$.event.event.eventID.string"));
          assertEquals("Project", JsonPath.read(json, "$.event.event.verbatimEventType.string"));
          assertEquals(
              "Project / Event",
              JsonPath.read(json, "$.event.event.eventTypeHierarchyJoined.string"));
          assertEquals("1 / 1", JsonPath.read(json, "$.event.event.eventHierarchyJoined.string"));
          assertEquals(
              2, JsonPath.<Integer>read(json, "$.event.event.eventHierarchyLevels.int").intValue());
          assertEquals("1", JsonPath.read(json, "$.event.event.id.string"));
          assertEquals("EUROPE", JsonPath.read(json, "$.event.event.publishedByGbifRegion.string"));
          assertEquals("BG", JsonPath.read(json, "$.event.event.publishingCountry.string"));
          assertEquals(
              "nlbif2022.015", JsonPath.read(json, "$.event.event.projectIDJoined.string"));

          // Metadata checks
          assertEquals(
              "8d5fe649-f85e-43cc-a19c-2a9979a741ac",
              JsonPath.read(json, "$.metadata.metadata.datasetKey.string"));
          assertEquals(
              "Mid-winter count of water birds and other bird species in Bulgaria",
              JsonPath.read(json, "$.metadata.metadata.datasetTitle.string"));
          assertEquals(
              "Bulgarian Society for the Protection of Birds",
              JsonPath.read(json, "$.metadata.metadata.publisherTitle.string"));

          // notIssues should contain many entries and at least one known item
          java.util.List<String> notIssues =
              JsonPath.<java.util.List<String>>read(
                  json, "$.event.event.notIssues.array[*].element.string");
          org.junit.Assert.assertNotNull(notIssues);
          Assert.assertFalse(notIssues.isEmpty());
          org.junit.Assert.assertTrue(
              notIssues.contains("COORDINATE_PRECISION_UNCERTAINTY_MISMATCH"));

        } else if ("EVT-001".equals(eventId)) {
          // Child record checks (verbatim-derived fields)
          // some ParentJson shapes may nest these differently; try both nested and flat keys
          String eventID = null;
          try {
            eventID = JsonPath.read(json, "$.event.event.eventID.string");
          } catch (Exception ignored) {
          }

          assertEquals("EVT-001", eventID);

          // Parent relationship and type
          assertEquals("EVT-000", JsonPath.read(json, "$.event.event.parentEventID.string"));
          assertEquals("Survey", JsonPath.read(json, "$.event.event.verbatimEventType.string"));

          // Event core fields (try nested paths used in ParentJsonRecord)
          assertEquals("2024-06-15T00:00", JsonPath.read(json, "$.event.event.eventDateSingle.string"));

          // time, protocol, effort, remarks
          assertEquals(
              "visual survey",
              JsonPath.read(json, "$.event.event.samplingProtocol.array[0].element.string"));
          assertEquals("Kenya", JsonPath.read(json, "$.event.event.country.string"));
          assertEquals("KE", JsonPath.read(json, "$.event.event.countryCode.string"));

          // coordinates: prefer numeric paths, fallback to verbatim strings
          Double lat = JsonPath.<Double>read(json, "$.event.event.decimalLatitude.double");
          Double lon = JsonPath.<Double>read(json, "$.event.event.decimalLongitude.double");
          org.junit.Assert.assertEquals(-1.4061d, lat, 0.0001);
          org.junit.Assert.assertEquals(35.0128d, lon, 0.0001);




        } else {
          throw new IllegalStateException(
              "Unexpected eventId in JSON: " + eventId + " json: " + json);
        }
      }
    }
  }

  private static String toJson(GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);

    writer.write(record, encoder);
    encoder.flush();

    return out.toString(StandardCharsets.UTF_8);
  }

  // java
  private void generateVerbatimAvro(String uuid, int attempt) throws IOException {

    ExtendedRecord parentEr =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreRowType(DwcTerm.Event.qualifiedName())
            .setCoreTerms(
                Map.of(
                    DwcTerm.eventID.qualifiedName(), "EVT-000",
                    DwcTerm.eventType.qualifiedName(), "Project"))
            .build();
    parentEr.setCoreRowType(DwcTerm.Event.qualifiedName());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("2").build();
    er.setCoreRowType(DwcTerm.Event.qualifiedName());

    Map<String, String> eventCoreTerms = new HashMap<>();
    eventCoreTerms.put(DwcTerm.eventID.qualifiedName(), "EVT-001");
    eventCoreTerms.put(DwcTerm.parentEventID.qualifiedName(), "EVT-000");
    eventCoreTerms.put(DwcTerm.eventType.qualifiedName(), "Survey");
    eventCoreTerms.put(DwcTerm.eventDate.qualifiedName(), "2024-06-15");
    eventCoreTerms.put(DwcTerm.eventTime.qualifiedName(), "10:30:00Z");
    eventCoreTerms.put(DwcTerm.samplingProtocol.qualifiedName(), "visual survey");
    eventCoreTerms.put(DwcTerm.samplingEffort.qualifiedName(), "2 hours");
    eventCoreTerms.put(DwcTerm.eventRemarks.qualifiedName(), "Dummy event for testing");
    eventCoreTerms.put(DwcTerm.country.qualifiedName(), "Kenya");
    eventCoreTerms.put(DwcTerm.countryCode.qualifiedName(), "KE");
    eventCoreTerms.put(DwcTerm.stateProvince.qualifiedName(), "Narok");
    eventCoreTerms.put(DwcTerm.locality.qualifiedName(), "Maasai Mara National Reserve");
    eventCoreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), "-1.4061");
    eventCoreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), "35.0128");

    er.setCoreTerms(eventCoreTerms);

    // Occurrence extension
    Map<String, String> occurrenceTerms = new HashMap<>();
    occurrenceTerms.put(DwcTerm.eventID.qualifiedName(), "EVT-001");
    occurrenceTerms.put(DwcTerm.occurrenceID.qualifiedName(), "occ-123456");
    occurrenceTerms.put(DwcTerm.scientificName.qualifiedName(), "Panthera leo");
    occurrenceTerms.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    occurrenceTerms.put(DwcTerm.phylum.qualifiedName(), "Chordata");
    occurrenceTerms.put(DwcTerm.class_.qualifiedName(), "Mammalia");
    occurrenceTerms.put(DwcTerm.order.qualifiedName(), "Carnivora");
    occurrenceTerms.put(DwcTerm.family.qualifiedName(), "Felidae");
    occurrenceTerms.put(DwcTerm.genus.qualifiedName(), "Panthera");
    occurrenceTerms.put(DwcTerm.specificEpithet.qualifiedName(), "leo");
    occurrenceTerms.put(DwcTerm.taxonRank.qualifiedName(), "species");
    occurrenceTerms.put(DwcTerm.basisOfRecord.qualifiedName(), "HumanObservation");

    // Humboldt extension
    Map<String, String> humboldtTerms = new HashMap<>();
    humboldtTerms.put(DwcTerm.eventID.qualifiedName(), "EVT-001");
    humboldtTerms.put(EcoTerm.targetLifeStageScope.name(), "Adult");
    humboldtTerms.put(EcoTerm.excludedLifeStageScope.name(), "Juvenile");
    humboldtTerms.put(EcoTerm.isLifeStageScopeFullyReported.name(), "true");
    humboldtTerms.put(EcoTerm.targetDegreeOfEstablishmentScope.name(), "Established");
    humboldtTerms.put(EcoTerm.excludedDegreeOfEstablishmentScope.name(), "Introduced");
    humboldtTerms.put(EcoTerm.isDegreeOfEstablishmentScopeFullyReported.name(), "false");
    humboldtTerms.put(EcoTerm.targetGrowthFormScope.name(), "Tree");
    humboldtTerms.put(EcoTerm.excludedGrowthFormScope.name(), "Shrub");
    humboldtTerms.put(EcoTerm.isGrowthFormScopeFullyReported.name(), "true");
    humboldtTerms.put(EcoTerm.hasNonTargetOrganisms.name(), "no");
    humboldtTerms.put(EcoTerm.verbatimTargetScope.name(), "canopy");
    humboldtTerms.put(EcoTerm.identifiedBy.name(), "Dr. Jane Doe");
    humboldtTerms.put(EcoTerm.identificationReferences.name(), "ID-REF-001");
    humboldtTerms.put(EcoTerm.compilationTypes.name(), "Aggregate");
    humboldtTerms.put(EcoTerm.compilationSourceTypes.name(), "Survey");
    humboldtTerms.put(EcoTerm.inventoryTypes.name(), "Plot-based");
    humboldtTerms.put(EcoTerm.protocolNames.name(), "Standard Transect");
    humboldtTerms.put(EcoTerm.protocolDescriptions.name(), "Visual survey along transect");
    humboldtTerms.put(EcoTerm.protocolReferences.name(), "Protocol-2024");
    humboldtTerms.put(EcoTerm.isAbundanceReported.name(), "true");
    humboldtTerms.put(EcoTerm.isAbundanceCapReported.name(), "false");
    humboldtTerms.put(EcoTerm.abundanceCap.name(), "100");
    humboldtTerms.put(EcoTerm.hasVouchers.name(), "yes");
    humboldtTerms.put(EcoTerm.voucherInstitutions.name(), "National Museum");
    humboldtTerms.put(EcoTerm.hasMaterialSamples.name(), "yes");
    humboldtTerms.put(EcoTerm.materialSampleTypes.name(), "tissue;soil");
    humboldtTerms.put(EcoTerm.samplingPerformedBy.name(), "Team A");
    humboldtTerms.put(EcoTerm.isSamplingEffortReported.name(), "true");
    humboldtTerms.put(EcoTerm.samplingEffortProtocol.name(), "Timed search");
    humboldtTerms.put(EcoTerm.samplingEffortValue.name(), "2");
    humboldtTerms.put(EcoTerm.samplingEffortUnit.name(), "hours");

    er.setExtensions(
        Map.of(
            Extension.OCCURRENCE.getRowType(), List.of(occurrenceTerms),
            Extension.HUMBOLDT.getRowType(), List.of(humboldtTerms)
        )
    );

    // Avro schema for ExtendedRecord
    Schema schema = ReflectData.AllowNull.get().getSchema(ExtendedRecord.class);

    // Output Avro file path (use .avro)
    String outputFile = getClass().getResource("/").getFile();
    String outputPath = outputFile + "/" + uuid + "/" + attempt + "/verbatim.avro";

    // Ensure parent directories exist
    java.nio.file.Path parent = java.nio.file.Paths.get(outputPath).getParent();
    if (parent != null) {
      java.nio.file.Files.createDirectories(parent);
    }

    // Write Avro container file using ReflectDatumWriter and DataFileWriter
    org.apache.avro.io.DatumWriter<ExtendedRecord> datumWriter =
        new org.apache.avro.reflect.ReflectDatumWriter<>(ExtendedRecord.class);

    try (org.apache.avro.file.DataFileWriter<ExtendedRecord> dataFileWriter =
        new org.apache.avro.file.DataFileWriter<>(datumWriter)) {

      // Use Snappy codec (optional)
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());

      dataFileWriter.create(schema, new java.io.File(outputPath));
      dataFileWriter.append(parentEr);
      dataFileWriter.append(er);
    }
  }
}
