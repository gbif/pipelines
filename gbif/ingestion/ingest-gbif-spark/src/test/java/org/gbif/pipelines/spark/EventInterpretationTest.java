package org.gbif.pipelines.spark;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
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

    // TODO: missing assert results
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

    er.setExtensions(Map.of("http://rs.tdwg.org/dwc/terms/Occurrence", List.of(occurrenceTerms)));

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
