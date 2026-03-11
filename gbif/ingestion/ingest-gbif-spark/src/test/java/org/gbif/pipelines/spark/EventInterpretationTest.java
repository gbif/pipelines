package org.gbif.pipelines.spark;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
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

    // load parquet into spark sql dataframe
    SparkSession spark = SparkSession.builder().appName("EventInterpretationTest").master("local[*]").getOrCreate();
    Dataset<ParentJsonRecord> jsonDf =  spark.read().parquet(parquetFile.toString()).as(Encoders.bean(ParentJsonRecord.class));
    List<String> jsonRecord = jsonDf.toJSON().collectAsList();
    for (String json : jsonRecord) {

      String eventId = JsonPath.read(json, "$.event.eventID");
      //write json in pretty print to file for debugging
      java.nio.file.Files.write(java.nio.file.Path.of(
              outputFile + "event"+ eventId+".json"), new ObjectMapper().readTree(json).toPrettyString()
              .getBytes(StandardCharsets.UTF_8));

      if("EVT-000".equals(eventId)) {
        // Parent record checks
        assertEquals("EVT-000", JsonPath.read(json, "$.event.eventID"));
        assertEquals("Project", JsonPath.read(json, "$.event.eventType.concept"));
      } else if ("EVT-001".equals(eventId)) {
        // Child record checks
        assertEquals("EVT-001", JsonPath.read(json, "$.event.eventID"));
        assertEquals("Survey", JsonPath.read(json, "$.event.eventType.concept"));
        assertEquals("EVT-000", JsonPath.read(json, "$.event.parentEventID"));
        assertEquals(Map.of("gte", "2024-06-15T00:00:00.000", "lte", "2024-06-15T23:59:59.999"), JsonPath.read(json, "$.event.eventDate"));
        assertEquals(List.of("visual survey"), JsonPath.read(json, "$.event.samplingProtocol"));
        assertEquals("Kenya", JsonPath.read(json, "$.event.country"));
        assertEquals("KE", JsonPath.read(json, "$.event.countryCode"));
        assertEquals("Narok", JsonPath.read(json, "$.event.stateProvince"));
        assertEquals("Maasai Mara National Reserve", JsonPath.read(json, "$.event.locality"));
        assertEquals(-1.4061d, JsonPath.<Double>read(json, "$.event.decimalLatitude"), 0.0001);
        assertEquals(35.0128d, JsonPath.<Double>read(json, "$.event.decimalLongitude"), 0.0001);

        assertEquals(167, JsonPath.<Integer>read(json, "$.event.endDayOfYear").intValue());
        assertEquals("2024-06-15T00:00:00.000", JsonPath.read(json, "$.event.eventDate.gte"));
        assertEquals("2024-06-15T23:59:59.999", JsonPath.read(json, "$.event.eventDate.lte"));
        assertEquals("2024-06-15", JsonPath.read(json, "$.event.eventDateInterval"));
        assertEquals("2024-06-15T00:00", JsonPath.read(json, "$.event.eventDateSingle"));

        // event hierarchy and ids
        assertEquals(List.of("2", "2"), JsonPath.<List<String>>read(json, "$.event.eventHierarchy"));
        assertEquals("2 / 2", JsonPath.read(json, "$.event.eventHierarchyJoined"));
        assertEquals(2, JsonPath.<Integer>read(json, "$.event.eventHierarchyLevels").intValue());
        assertEquals("EVT-001", JsonPath.read(json, "$.event.eventID"));
        assertEquals("2", JsonPath.read(json, "$.event.id"));

        // eventType and lineage
        assertEquals("Survey", JsonPath.read(json, "$.event.eventType.concept"));
        assertEquals(List.of("Event", "Project", "Survey"), JsonPath.<List<String>>read(json, "$.event.eventType.lineage"));
        assertEquals(List.of("Survey", "Survey"), JsonPath.<List<String>>read(json, "$.event.eventTypeHierarchy"));
        assertEquals("Survey / Survey", JsonPath.read(json, "$.event.eventTypeHierarchyJoined"));

        // extensions & flags
        List<String> exts = JsonPath.<List<String>>read(json, "$.event.extensions");
        assertNotNull(exts);
        assertTrue(exts.contains("http://rs.tdwg.org/eco/terms/Event"));
        assertTrue(exts.contains("http://rs.tdwg.org/dwc/terms/Occurrence"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.hasCoordinate"));
        assertFalse(JsonPath.<Boolean>read(json, "$.event.hasGeospatialIssue"));

        // gadm & region/state
        assertEquals("Kenya", JsonPath.read(json, "$.event.gadm.level0Name"));
        assertEquals("Narok", JsonPath.read(json, "$.event.gadm.level1Name"));
        assertEquals("Kilgoris", JsonPath.read(json, "$.event.gadm.level2Name"));
        assertEquals("Kimintet", JsonPath.read(json, "$.event.gadm.level3Name"));
        assertEquals("AFRICA", JsonPath.read(json, "$.event.gbifRegion"));
        assertEquals("Narok", JsonPath.read(json, "$.event.stateProvince"));

        // issues (contains specific known issues)
        List<String> issues = JsonPath.<List<String>>read(json, "$.event.issues");
        assertNotNull(issues);
        assertTrue(issues.contains("HAS_MATERIAL_SAMPLES_MISMATCH"));
        assertTrue(issues.contains("GEODETIC_DATUM_ASSUMED_WGS84"));
        assertTrue(issues.contains("CONTINENT_DERIVED_FROM_COORDINATES"));

        // locality & temporal fields
        assertEquals("Maasai Mara National Reserve", JsonPath.read(json, "$.event.locality"));
        assertEquals(6, JsonPath.<Integer>read(json, "$.event.month").intValue());
        assertEquals(2024, JsonPath.<Integer>read(json, "$.event.year").intValue());
        assertEquals("0797506EVT-001", JsonPath.read(json, "$.event.yearMonthEventIDSort"));

        // parent & parentsLineage
        assertEquals("EVT-000", JsonPath.read(json, "$.event.parentEventID"));
        assertEquals("Survey", JsonPath.read(json, "$.event.parentsLineage[0].eventType"));
        assertEquals("2", JsonPath.read(json, "$.event.parentsLineage[0].id"));

        // project & publishing info
        assertEquals(List.of("nlbif2022.015"), JsonPath.<List<String>>read(json, "$.event.projectID"));
        assertEquals("nlbif2022.015", JsonPath.read(json, "$.event.projectIDJoined"));
        assertEquals("EUROPE", JsonPath.read(json, "$.event.publishedByGbifRegion"));
        assertEquals("BG", JsonPath.read(json, "$.event.publishingCountry"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.repatriated"));

        // sampling protocol / effort / coordinates summary
        assertEquals(List.of("visual survey"), JsonPath.<List<String>>read(json, "$.event.samplingProtocol"));
        assertEquals("visual survey", JsonPath.read(json, "$.event.samplingProtocolJoined"));
        assertEquals("POINT (35.0128 -1.4061)", JsonPath.read(json, "$.event.scoordinates"));
        assertEquals(167, JsonPath.<Integer>read(json, "$.event.startDayOfYear").intValue());

        // survey / verbatim event type info
        assertEquals("EVT-001", JsonPath.read(json, "$.event.surveyID"));
        assertEquals("Survey", JsonPath.read(json, "$.event.verbatimEventType"));
        assertEquals(List.of("Survey", "Survey"), JsonPath.<List<String>>read(json, "$.event.verbatimEventTypeHierarchy"));
        assertEquals("Survey / Survey", JsonPath.read(json, "$.event.verbatimEventTypeHierarchyJoined"));

        // humboldt
        assertEquals(100, JsonPath.<Integer>read(json, "$.event.humboldt[0].abundanceCap").intValue());
        assertEquals(List.of("Survey"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].compilationSourceTypes"));
        assertEquals(List.of("Aggregate"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].compilationTypes"));
        assertEquals(List.of("Plot-based"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].inventoryTypes"));
        assertEquals(List.of("Shrub"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].excludedGrowthFormScope"));
        assertEquals("Juvenile", JsonPath.read(json, "$.event.humboldt[0].excludedLifeStageScope[0].concept"));
        assertFalse(JsonPath.<Boolean>read(json, "$.event.humboldt[0].hasMaterialSamples"));
        assertFalse(JsonPath.<Boolean>read(json, "$.event.humboldt[0].hasNonTargetOrganisms"));
        assertFalse(JsonPath.<Boolean>read(json, "$.event.humboldt[0].hasVouchers"));
        assertEquals("hours", JsonPath.read(json, "$.event.humboldt[0].samplingEffortUnit"));
        assertEquals(2.0d, JsonPath.<Double>read(json, "$.event.humboldt[0].samplingEffortValue"), 1e-6);
        assertEquals(List.of("Team A"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].samplingPerformedBy"));
        assertEquals(List.of("Visual survey along transect"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].protocolDescriptions"));
        assertEquals(List.of("Standard Transect"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].protocolNames"));
        assertEquals(List.of("Protocol-2024"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].protocolReferences"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.humboldt[0].isAbundanceReported"));
        assertFalse(JsonPath.<Boolean>read(json, "$.event.humboldt[0].isAbundanceCapReported"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.humboldt[0].isGrowthFormScopeFullyReported"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.humboldt[0].isLifeStageScopeFullyReported"));
        assertTrue(JsonPath.<Boolean>read(json, "$.event.humboldt[0].isSamplingEffortReported"));
        assertEquals(List.of("tissue;soil"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].materialSampleTypes"));
        assertEquals(12, JsonPath.<Integer>read(json, "$.event.humboldt[0].siteCount").intValue());
        assertEquals("established", JsonPath.read(json, "$.event.humboldt[0].targetDegreeOfEstablishmentScope[0].concept"));
        assertEquals(List.of("Tree"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].targetGrowthFormScope"));
        assertEquals("Adult", JsonPath.read(json, "$.event.humboldt[0].targetLifeStageScope[0].concept"));
        assertEquals(List.of("National Museum"), JsonPath.<List<String>>read(json, "$.event.humboldt[0].voucherInstitutions"));

        // eventInherited
        assertEquals(List.of("Survey"), JsonPath.<List<String>>read(json, "$.eventInherited.eventType"));
        assertEquals("2", JsonPath.read(json, "$.eventInherited.id"));

        // top-level ids & timing (exact match to test resource)
        assertEquals("2", JsonPath.read(json, "$.id"));

        // locationInherited
        assertEquals("KE", JsonPath.read(json, "$.locationInherited.countryCode"));
        assertEquals(-1.4061d, JsonPath.<Double>read(json, "$.locationInherited.decimalLatitude"), 1e-6);
        assertEquals(35.0128d, JsonPath.<Double>read(json, "$.locationInherited.decimalLongitude"), 1e-6);
        assertEquals("2", JsonPath.read(json, "$.locationInherited.id"));
        assertEquals("Narok", JsonPath.read(json, "$.locationInherited.stateProvince"));

        // metadata
        assertEquals("8d5fe649-f85e-43cc-a19c-2a9979a741ac", JsonPath.read(json, "$.metadata.datasetKey"));
        assertEquals("BG", JsonPath.read(json, "$.metadata.datasetPublishingCountry"));
        assertEquals("Mid-winter count of water birds and other bird species in Bulgaria", JsonPath.read(json, "$.metadata.datasetTitle"));
        assertEquals("8dae0f8c-12bc-444d-b889-6b177550a8b2", JsonPath.read(json, "$.metadata.endorsingNodeKey"));
        assertEquals("fbca90e3-8aed-48b1-84e3-369afbd000ce", JsonPath.read(json, "$.metadata.hostingOrganizationKey"));
        assertEquals("17a83780-3060-4851-9d6f-029d5fcb81c9", JsonPath.read(json, "$.metadata.installationKey"));
        assertEquals("EML", JsonPath.read(json, "$.metadata.protocol"));
        assertEquals("Bulgarian Society for the Protection of Birds", JsonPath.read(json, "$.metadata.publisherTitle"));
        assertEquals("d1fec91e-5b53-4cff-9b31-10fed530a3c3", JsonPath.read(json, "$.metadata.publishingOrganizationKey"));

        // temporalInherited
        assertEquals(15, JsonPath.<Integer>read(json, "$.temporalInherited.day").intValue());
        assertEquals("2", JsonPath.read(json, "$.temporalInherited.id"));
        assertEquals(6, JsonPath.<Integer>read(json, "$.temporalInherited.month").intValue());
        assertEquals(2024, JsonPath.<Integer>read(json, "$.temporalInherited.year").intValue());

        // type
        assertEquals("event", JsonPath.read(json, "$.type"));

        // verbatim - core map
        assertEquals("EVT-001", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventID']"));
        assertEquals("Kenya", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/country']"));
        assertEquals("EVT-000", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/parentEventID']"));
        assertEquals("visual survey", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/samplingProtocol']"));
        assertEquals("Maasai Mara National Reserve", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/locality']"));
        assertEquals("2 hours", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/samplingEffort']"));
        assertEquals("2024-06-15", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventDate']"));
        assertEquals("Survey", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventType']"));
        assertEquals("Dummy event for testing", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventRemarks']"));
        assertEquals("35.0128", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/decimalLongitude']"));
        assertEquals("-1.4061", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/decimalLatitude']"));
        assertEquals("KE", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/countryCode']"));
        assertEquals("10:30:00Z", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventTime']"));
        assertEquals("Narok", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/stateProvince']"));

        // verbatim - extensions map (eco/Event)
        assertEquals("100", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/abundanceCap']"));
        assertEquals("EVT-001", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/dwc/terms/eventID']"));
        assertEquals("false", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/isAbundanceCapReported']"));
        assertEquals("Dr. Jane Doe", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/identifiedBy']"));
        assertEquals("Adult", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/targetLifeStageScope']"));
        assertEquals("Tree", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/targetGrowthFormScope']"));
        assertEquals("yes", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/hasMaterialSamples']"));
        assertEquals("hours", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/samplingEffortUnit']"));
        assertEquals("Plot-based", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/inventoryTypes']"));
        assertEquals("Established", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/targetDegreeOfEstablishmentScope']"));
        assertEquals("Shrub", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/excludedGrowthFormScope']"));
        assertEquals("Juvenile", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/excludedLifeStageScope']"));
        assertEquals("Protocol-2024", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/protocolReferences']"));
        assertEquals("true", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/isSamplingEffortReported']"));
        assertEquals("Introduced", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/excludedDegreeOfEstablishmentScope']"));
        assertEquals("National Museum", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/voucherInstitutions']"));
        assertEquals("false", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/isDegreeOfEstablishmentScopeFullyReported']"));
        assertEquals("2", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/samplingEffortValue']"));
        assertEquals("no", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/hasNonTargetOrganisms']"));
        assertEquals("Visual survey along transect", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/protocolDescriptions']"));
        assertEquals("true", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/isAbundanceReported']"));
        assertEquals("canopy", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/verbatimTargetScope']"));
        assertEquals("true", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/isLifeStageScopeFullyReported']"));
        assertEquals("ID-REF-001", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/identificationReferences']"));
        assertEquals("Survey", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/compilationSourceTypes']"));
        assertEquals("12", JsonPath.read(json, "$.verbatim.extensions['http://rs.tdwg.org/eco/terms/Event'][0]['http://rs.tdwg.org/eco/terms/siteCount']"));
      } else {
        throw new IllegalStateException("Unexpected eventID: " + eventId);
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
