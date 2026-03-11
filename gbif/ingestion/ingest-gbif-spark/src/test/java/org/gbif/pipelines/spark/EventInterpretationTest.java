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
          // Top-level identifiers & derived metadata
          assertEquals("2", JsonPath.read(json, "$.id.string"));
          assertEquals(1, JsonPath.<Integer>read(json, "$.crawlId.int").intValue());
          assertEquals("2", JsonPath.read(json, "$.derivedMetadata.derivedMetadata.id.string"));
          assertEquals(
              "2024-06-15T00:00:00.000",
              JsonPath.read(
                  json,
                  "$.derivedMetadata.derivedMetadata.temporalCoverage.temporalCoverage.gte.string"));
          assertEquals(
              "2024-06-15T23:59:59.999",
              JsonPath.read(
                  json,
                  "$.derivedMetadata.derivedMetadata.temporalCoverage.temporalCoverage.lte.string"));
          assertEquals(
              "POINT(-1.4061 35.0128)",
              JsonPath.read(json, "$.derivedMetadata.derivedMetadata.wktConvexHull.string"));

          // Event core fields
          assertEquals("EVT-001", JsonPath.read(json, "$.event.event.eventID.string"));
          assertEquals("AFRICA", JsonPath.read(json, "$.event.event.continent.string"));
          // coordinates numeric
          Double lat = JsonPath.<Double>read(json, "$.event.event.coordinates.coordinates.lat");
          Double lon = JsonPath.<Double>read(json, "$.event.event.coordinates.coordinates.lon");
          org.junit.Assert.assertEquals(-1.4061d, lat, 1e-6);
          org.junit.Assert.assertEquals(35.0128d, lon, 1e-6);
          // numeric latitude/longitude also present
          org.junit.Assert.assertEquals(
              -1.4061d, JsonPath.<Double>read(json, "$.event.event.decimalLatitude.double"), 1e-6);
          org.junit.Assert.assertEquals(
              35.0128d, JsonPath.<Double>read(json, "$.event.event.decimalLongitude.double"), 1e-6);

          assertEquals("Kenya", JsonPath.read(json, "$.event.event.country.string"));
          assertEquals("KE", JsonPath.read(json, "$.event.event.countryCode.string"));
          assertEquals(15, JsonPath.<Integer>read(json, "$.event.event.day.int").intValue());
          assertEquals(
              167, JsonPath.<Integer>read(json, "$.event.event.endDayOfYear.int").intValue());
          assertEquals("2024-06-15", JsonPath.read(json, "$.event.event.eventDateInterval.string"));
          assertEquals(
              "2024-06-15T00:00", JsonPath.read(json, "$.event.event.eventDateSingle.string"));
          assertEquals(
              "2024-06-15T00:00:00.000",
              JsonPath.read(json, "$.event.event.eventDate.eventDate.gte.string"));
          assertEquals(
              "2024-06-15T23:59:59.999",
              JsonPath.read(json, "$.event.event.eventDate.eventDate.lte.string"));

          // event hierarchy
          assertEquals(
              "2", JsonPath.read(json, "$.event.event.eventHierarchy.array[0].element.string"));
          assertEquals(
              "2", JsonPath.read(json, "$.event.event.eventHierarchy.array[1].element.string"));
          assertEquals("2 / 2", JsonPath.read(json, "$.event.event.eventHierarchyJoined.string"));
          assertEquals(
              2, JsonPath.<Integer>read(json, "$.event.event.eventHierarchyLevels.int").intValue());

          // eventType & type hierarchy
          assertEquals(
              "Event", JsonPath.read(json, "$.event.event.eventType.eventType.concept.string"));
          assertEquals(
              "Event",
              JsonPath.read(
                  json, "$.event.event.eventType.eventType.lineage.array[0].element.string"));
          assertEquals(
              "Survey",
              JsonPath.read(json, "$.event.event.eventTypeHierarchy.array[0].element.string"));
          assertEquals(
              "Event",
              JsonPath.read(json, "$.event.event.eventTypeHierarchy.array[1].element.string"));
          assertEquals(
              "Survey / Event",
              JsonPath.read(json, "$.event.event.eventTypeHierarchyJoined.string"));

          // extensions list on event
          List<String> eventExts =
              JsonPath.<List<String>>read(json, "$.event.event.extensions.array[*].element.string");
          assertNotNull(eventExts);
          org.junit.Assert.assertTrue(eventExts.contains("http://rs.tdwg.org/eco/terms/Event"));
          org.junit.Assert.assertTrue(
              eventExts.contains("http://rs.tdwg.org/dwc/terms/Occurrence"));

          // GADM and region/state
          assertEquals("Kenya", JsonPath.read(json, "$.event.event.gadm.gadm.level0Name.string"));
          assertEquals("Narok", JsonPath.read(json, "$.event.event.gadm.gadm.level1Name.string"));
          assertEquals(
              "Kilgoris", JsonPath.read(json, "$.event.event.gadm.gadm.level2Name.string"));
          assertEquals(
              "Kimintet", JsonPath.read(json, "$.event.event.gadm.gadm.level3Name.string"));
          assertEquals("AFRICA", JsonPath.read(json, "$.event.event.gbifRegion.string"));
          assertEquals("Narok", JsonPath.read(json, "$.event.event.stateProvince.string"));

          // flags & basic meta
          org.junit.Assert.assertTrue(
              JsonPath.<Boolean>read(json, "$.event.event.hasCoordinate.boolean"));
          org.junit.Assert.assertFalse(
              JsonPath.<Boolean>read(json, "$.event.event.hasGeospatialIssue.boolean"));
          assertEquals("2", JsonPath.read(json, "$.event.event.id.string"));

          // issues
          List<String> issues =
              JsonPath.<List<String>>read(json, "$.event.event.issues.array[*].element.string");
          assertNotNull(issues);
          org.junit.Assert.assertTrue(issues.contains("GEODETIC_DATUM_ASSUMED_WGS84"));
          org.junit.Assert.assertTrue(issues.contains("CONTINENT_DERIVED_FROM_COORDINATES"));

          // locality and temporal
          assertEquals(
              "Maasai Mara National Reserve", JsonPath.read(json, "$.event.event.locality.string"));
          assertEquals(6, JsonPath.<Integer>read(json, "$.event.event.month.int").intValue());
          assertEquals(2024, JsonPath.<Integer>read(json, "$.event.event.year.int").intValue());
          assertEquals(
              "0797506EVT-001", JsonPath.read(json, "$.event.event.yearMonthEventIDSort.string"));

          // parent and parentsLineage
          assertEquals("EVT-000", JsonPath.read(json, "$.event.event.parentEventID.string"));
          Object parentsElem = JsonPath.read(json, "$.event.event.parentsLineage.array[0].element");
          org.junit.Assert.assertNotNull(parentsElem);
          org.junit.Assert.assertTrue(parentsElem.toString().contains("Survey"));
          org.junit.Assert.assertTrue(parentsElem.toString().contains("2"));

          // project & publishing info
          assertEquals(
              "nlbif2022.015",
              JsonPath.read(json, "$.event.event.projectID.array[0].element.string"));
          assertEquals(
              "nlbif2022.015", JsonPath.read(json, "$.event.event.projectIDJoined.string"));
          assertEquals("EUROPE", JsonPath.read(json, "$.event.event.publishedByGbifRegion.string"));
          assertEquals("BG", JsonPath.read(json, "$.event.event.publishingCountry.string"));
          org.junit.Assert.assertTrue(
              JsonPath.<Boolean>read(json, "$.event.event.repatriated.boolean"));

          // sampling protocol / effort / coordinates summary
          assertEquals(
              "visual survey",
              JsonPath.read(json, "$.event.event.samplingProtocol.array[0].element.string"));
          assertEquals(
              "visual survey", JsonPath.read(json, "$.event.event.samplingProtocolJoined.string"));
          assertEquals(
              "POINT (35.0128 -1.4061)", JsonPath.read(json, "$.event.event.scoordinates.string"));
          assertEquals(
              167, JsonPath.<Integer>read(json, "$.event.event.startDayOfYear.int").intValue());

          // Humboldt interpretation
          String humboldtBase = "$.event.event.humboldt.array[0].element";
          String humElement = humboldtBase + "['element2.element']";

          assertEquals(
              100, JsonPath.<Integer>read(json, humElement + "['abundanceCap'].int").intValue());
          assertEquals(
              "Survey",
              JsonPath.read(
                  json, humElement + "['compilationSourceTypes'].array[0].element.string"));
          assertEquals(
              "Aggregate",
              JsonPath.read(json, humElement + "['compilationTypes'].array[0].element.string"));
          assertEquals(
              "Plot-based",
              JsonPath.read(json, humElement + "['inventoryTypes'].array[0].element.string"));
          assertEquals(
              "Shrub",
              JsonPath.read(
                  json, humElement + "['excludedGrowthFormScope'].array[0].element.string"));
          assertEquals(
              "Juvenile",
              JsonPath.read(
                  json,
                  humElement
                      + "['excludedLifeStageScope'].array[0].element['element5.element'].concept.string"));
          assertEquals(
              "established",
              JsonPath.read(
                  json,
                  humElement
                      + "['targetDegreeOfEstablishmentScope'].array[0].element['element8.element'].concept.string"));
          assertEquals(
              "Tree",
              JsonPath.read(
                  json, humElement + "['targetGrowthFormScope'].array[0].element.string"));
          assertEquals(
              "Adult",
              JsonPath.read(
                  json,
                  humElement
                      + "['targetLifeStageScope'].array[0].element['element9.element'].concept.string"));

          assertEquals(
              "National Museum",
              JsonPath.read(json, humElement + "['voucherInstitutions'].array[0].element.string"));
          assertEquals(
              12, JsonPath.<Integer>read(json, humElement + "['siteCount'].int").intValue());
          assertEquals("hours", JsonPath.read(json, humElement + "['samplingEffortUnit'].string"));
          assertEquals(
              2.0,
              JsonPath.<Double>read(json, humElement + "['samplingEffortValue'].double"),
              1e-6);
          assertEquals(
              "Team A",
              JsonPath.read(json, humElement + "['samplingPerformedBy'].array[0].element.string"));

          assertEquals(
              "Visual survey along transect",
              JsonPath.read(json, humElement + "['protocolDescriptions'].array[0].element.string"));
          assertEquals(
              "Standard Transect",
              JsonPath.read(json, humElement + "['protocolNames'].array[0].element.string"));
          assertEquals(
              "Protocol-2024",
              JsonPath.read(json, humElement + "['protocolReferences'].array[0].element.string"));

          assertFalse(JsonPath.<Boolean>read(json, humElement + "['hasMaterialSamples'].boolean"));
          assertFalse(JsonPath.<Boolean>read(json, humElement + "['hasVouchers'].boolean"));
          assertFalse(
              JsonPath.<Boolean>read(json, humElement + "['hasNonTargetOrganisms'].boolean"));
          assertTrue(JsonPath.<Boolean>read(json, humElement + "['isAbundanceReported'].boolean"));
          assertFalse(
              JsonPath.<Boolean>read(json, humElement + "['isAbundanceCapReported'].boolean"));
          assertTrue(
              JsonPath.<Boolean>read(
                  json, humElement + "['isGrowthFormScopeFullyReported'].boolean"));
          assertTrue(
              JsonPath.<Boolean>read(
                  json, humElement + "['isLifeStageScopeFullyReported'].boolean"));
          assertTrue(
              JsonPath.<Boolean>read(json, humElement + "['isSamplingEffortReported'].boolean"));
          // humboldt array exists (contains an element with nested element2.element object)
          Object hum = JsonPath.read(json, "$.event.event.humboldt.array[0].element");
          org.junit.Assert.assertNotNull(hum);
          org.junit.Assert.assertTrue(hum.toString().contains("element2.element"));

          // metadata top-level
          assertEquals(
              "8d5fe649-f85e-43cc-a19c-2a9979a741ac",
              JsonPath.read(json, "$.metadata.metadata.datasetKey.string"));
          assertEquals(
              "Mid-winter count of water birds and other bird species in Bulgaria",
              JsonPath.read(json, "$.metadata.metadata.datasetTitle.string"));
          assertEquals(
              "Bulgarian Society for the Protection of Birds",
              JsonPath.read(json, "$.metadata.metadata.publisherTitle.string"));
          assertEquals("EML", JsonPath.read(json, "$.metadata.metadata.protocol.string"));
          assertEquals(
              "d1fec91e-5b53-4cff-9b31-10fed530a3c3",
              JsonPath.read(json, "$.metadata.metadata.publishingOrganizationKey.string"));
          assertEquals(
              "BG", JsonPath.read(json, "$.metadata.metadata.datasetPublishingCountry.string"));
          assertEquals(
              "8dae0f8c-12bc-444d-b889-6b177550a8b2",
              JsonPath.read(json, "$.metadata.metadata.endorsingNodeKey.string"));
          assertEquals(
              "fbca90e3-8aed-48b1-84e3-369afbd000ce",
              JsonPath.read(json, "$.metadata.metadata.hostingOrganizationKey.string"));
          assertEquals(
              "17a83780-3060-4851-9d6f-029d5fcb81c9",
              JsonPath.read(json, "$.metadata.metadata.installationKey.string"));

          // locationInherited checks
          assertEquals(
              "KE",
              JsonPath.read(json, "$.locationInherited.locationInherited.countryCode.string"));
          org.junit.Assert.assertEquals(
              -1.4061d,
              JsonPath.<Double>read(
                  json, "$.locationInherited.locationInherited.decimalLatitude.double"),
              1e-6);
          org.junit.Assert.assertEquals(
              35.0128d,
              JsonPath.<Double>read(
                  json, "$.locationInherited.locationInherited.decimalLongitude.double"),
              1e-6);
          assertEquals("2", JsonPath.read(json, "$.locationInherited.locationInherited.id.string"));

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
    humboldtTerms.put(EcoTerm.siteCount.qualifiedName(), "12");
    humboldtTerms.put(EcoTerm.targetLifeStageScope.qualifiedName(), "Adult");
    humboldtTerms.put(EcoTerm.excludedLifeStageScope.qualifiedName(), "Juvenile");
    humboldtTerms.put(EcoTerm.isLifeStageScopeFullyReported.qualifiedName(), "true");
    humboldtTerms.put(EcoTerm.targetDegreeOfEstablishmentScope.qualifiedName(), "Established");
    humboldtTerms.put(EcoTerm.excludedDegreeOfEstablishmentScope.qualifiedName(), "Introduced");
    humboldtTerms.put(EcoTerm.isDegreeOfEstablishmentScopeFullyReported.qualifiedName(), "false");
    humboldtTerms.put(EcoTerm.targetGrowthFormScope.qualifiedName(), "Tree");
    humboldtTerms.put(EcoTerm.excludedGrowthFormScope.qualifiedName(), "Shrub");
    humboldtTerms.put(EcoTerm.isGrowthFormScopeFullyReported.qualifiedName(), "true");
    humboldtTerms.put(EcoTerm.hasNonTargetOrganisms.qualifiedName(), "no");
    humboldtTerms.put(EcoTerm.verbatimTargetScope.qualifiedName(), "canopy");
    humboldtTerms.put(EcoTerm.identifiedBy.qualifiedName(), "Dr. Jane Doe");
    humboldtTerms.put(EcoTerm.identificationReferences.qualifiedName(), "ID-REF-001");
    humboldtTerms.put(EcoTerm.compilationTypes.qualifiedName(), "Aggregate");
    humboldtTerms.put(EcoTerm.compilationSourceTypes.qualifiedName(), "Survey");
    humboldtTerms.put(EcoTerm.inventoryTypes.qualifiedName(), "Plot-based");
    humboldtTerms.put(EcoTerm.protocolNames.qualifiedName(), "Standard Transect");
    humboldtTerms.put(EcoTerm.protocolDescriptions.qualifiedName(), "Visual survey along transect");
    humboldtTerms.put(EcoTerm.protocolReferences.qualifiedName(), "Protocol-2024");
    humboldtTerms.put(EcoTerm.isAbundanceReported.qualifiedName(), "true");
    humboldtTerms.put(EcoTerm.isAbundanceCapReported.qualifiedName(), "false");
    humboldtTerms.put(EcoTerm.abundanceCap.qualifiedName(), "100");
    humboldtTerms.put(EcoTerm.hasVouchers.qualifiedName(), "yes");
    humboldtTerms.put(EcoTerm.voucherInstitutions.qualifiedName(), "National Museum");
    humboldtTerms.put(EcoTerm.hasMaterialSamples.qualifiedName(), "yes");
    humboldtTerms.put(EcoTerm.materialSampleTypes.qualifiedName(), "tissue;soil");
    humboldtTerms.put(EcoTerm.samplingPerformedBy.qualifiedName(), "Team A");
    humboldtTerms.put(EcoTerm.isSamplingEffortReported.qualifiedName(), "true");
    humboldtTerms.put(EcoTerm.samplingEffortProtocol.qualifiedName(), "Timed search");
    humboldtTerms.put(EcoTerm.samplingEffortValue.qualifiedName(), "2");
    humboldtTerms.put(EcoTerm.samplingEffortUnit.qualifiedName(), "hours");

    er.setExtensions(
        Map.of(
            Extension.OCCURRENCE.getRowType(), List.of(occurrenceTerms),
            Extension.HUMBOLDT.getRowType(), List.of(humboldtTerms)));

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
