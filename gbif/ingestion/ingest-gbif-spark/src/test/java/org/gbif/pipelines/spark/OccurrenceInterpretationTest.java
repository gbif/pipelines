package org.gbif.pipelines.spark;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.junit.*;

public class OccurrenceInterpretationTest extends MockedServicesTest {

  @Test
  public void test() throws Exception {

    URL testRootUrl = getClass().getResource("/");
    assert testRootUrl != null;
    String testResourcesRoot = testRootUrl.getFile();

    // change pipelines config YAML to URLs of the mocked services if needed
    String testUuid = "7683cc47-cb13-4bad-9614-387c66aa8df0";
    int attempt = 1;

    String basePath = getClass().getResource("/").getFile();

    // Generate test parquet files
    generateIdentifierParquet(basePath, testUuid, attempt);
    generateVerbatimParquet(basePath, testUuid, attempt);

    OccurrenceInterpretation.main(
        new String[] {
          "--appName=occurrence-interpretation-test",
          "--datasetId=" + testUuid,
          "--attempt=" + attempt,
          "--tripletValid=false",
          "--occurrenceIdValid=true",
          "--config=" + testResourcesRoot + "/pipelines.yaml",
          "--master=local[*]",
          "--numberOfShards=1",
          "--useSystemExit=false"
        });

    // check outputs exist
    assertParquetExists(basePath, testUuid, attempt, Directories.OCCURRENCE_HDFS);
    assertParquetExists(basePath, testUuid, attempt, Directories.OCCURRENCE_JSON);
    assertParquetExists(basePath, testUuid, attempt, Directories.SIMPLE_OCCURRENCE);
    assertParquetExists(basePath, testUuid, attempt, Directories.VERBATIM_EXT_FILTERED);

    // check hdfs outputs in detail
    checkHdfsTableOutputs(basePath, testUuid, attempt);
    checkJsonOutputs(basePath, testUuid, attempt);
  }

  private void checkJsonOutputs(String outputFile, String testUuid, int attempt) {}

  private void checkHdfsTableOutputs(String outputFile, String testUuid, int attempt)
      throws Exception {

    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve(Directories.OCCURRENCE_HDFS);

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
        validateRecord(record, EXPECTED_HDFS_FIELDS);
      }
    }
  }

  private static void validateRecord(GenericRecord record, Map<String, Object> expectedFields) {
    for (Map.Entry<String, Object> expected : expectedFields.entrySet()) {
      String field = expected.getKey();
      String expectedValue = expected.getValue().toString();
      String actualValue = record.get(field).toString();
      assertEquals(actualValue, expectedValue);
    }
  }

  private void assertParquetExists(String outputFile, String testUuid, int attempt, String subdir)
      throws IOException {
    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve(subdir);
    assertTrue("Missing output dir", Files.exists(dir));
    assertTrue(
        "Output dir empty",
        Files.list(dir)
            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
            .findFirst()
            .isPresent());
  }

  private void generateIdentifierParquet(String basePath, String uuid, int attempt)
      throws IOException {

    IdentifierRecord ir = new IdentifierRecord();
    ir.setId("1");
    ir.setInternalId("1");
    ir.setUniqueKey("1");

    // Write to parquet
    Schema schema = ReflectData.AllowNull.get().getSchema(IdentifierRecord.class);

    // Output Parquet file path
    String identifiersValidPath =
        basePath
            + "/"
            + uuid
            + "/"
            + attempt
            + "/"
            + Directories.IDENTIFIERS_VALID
            + "/identifers.parquet";
    String identifiersAbsentPath =
        basePath
            + "/"
            + uuid
            + "/"
            + attempt
            + "/"
            + Directories.IDENTIFIERS_ABSENT
            + "/identifers.parquet";

    try (ParquetWriter<IdentifierRecord> writer =
        AvroParquetWriter.<IdentifierRecord>builder(new Path(identifiersValidPath))
            .withSchema(schema)
            .withDataModel(ReflectData.get())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .withWriteMode(OVERWRITE)
            .build()) {
      writer.write(ir);
    }

    try (ParquetWriter<IdentifierRecord> writer =
        AvroParquetWriter.<IdentifierRecord>builder(new Path(identifiersAbsentPath))
            .withSchema(schema)
            .withDataModel(ReflectData.get())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .withWriteMode(OVERWRITE)
            .build()) {
      // write nothing
    }
  }

  private void generateVerbatimParquet(String basePath, String uuid, int attempt)
      throws IOException {
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").build();
    er.setCoreRowType(DwcTerm.Occurrence.qualifiedName());
    Map<String, String> coreTerms = new HashMap<>();

    // Taxon
    coreTerms.put(DwcTerm.scientificName.qualifiedName(), "Panthera leo");
    coreTerms.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    coreTerms.put(DwcTerm.phylum.qualifiedName(), "Chordata");
    coreTerms.put(DwcTerm.class_.qualifiedName(), "Mammalia");
    coreTerms.put(DwcTerm.order.qualifiedName(), "Carnivora");
    coreTerms.put(DwcTerm.family.qualifiedName(), "Felidae");
    coreTerms.put(DwcTerm.genus.qualifiedName(), "Panthera");
    coreTerms.put(DwcTerm.specificEpithet.qualifiedName(), "leo");
    coreTerms.put(DwcTerm.taxonRank.qualifiedName(), "species");

    // Occurrence
    coreTerms.put(DwcTerm.occurrenceID.qualifiedName(), "occ-123456");
    coreTerms.put(DwcTerm.basisOfRecord.qualifiedName(), "HumanObservation");
    coreTerms.put(DwcTerm.individualCount.qualifiedName(), "3");
    coreTerms.put(DwcTerm.sex.qualifiedName(), "male");
    coreTerms.put(DwcTerm.lifeStage.qualifiedName(), "adult");

    // Event
    coreTerms.put(DwcTerm.eventID.qualifiedName(), "event-98765");
    coreTerms.put(DwcTerm.eventDate.qualifiedName(), "2023-01-01");
    coreTerms.put(DwcTerm.year.qualifiedName(), String.valueOf(2023));
    coreTerms.put(DwcTerm.month.qualifiedName(), "7");
    coreTerms.put(DwcTerm.day.qualifiedName(), "14");

    // Location
    coreTerms.put(DwcTerm.country.qualifiedName(), "Kenya");
    coreTerms.put(DwcTerm.countryCode.qualifiedName(), "KE");
    coreTerms.put(DwcTerm.stateProvince.qualifiedName(), "Narok");
    coreTerms.put(DwcTerm.locality.qualifiedName(), "Maasai Mara National Reserve");
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), "-1.4061");
    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), "35.0128");
    coreTerms.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), "30");

    // Record-level metadata
    coreTerms.put(DwcTerm.recordedBy.qualifiedName(), "J. Smith");
    coreTerms.put(DwcTerm.identifiedBy.qualifiedName(), "A. Taxonomist");
    coreTerms.put(DwcTerm.institutionCode.qualifiedName(), "NMK");
    coreTerms.put(DwcTerm.collectionCode.qualifiedName(), "Mammals");
    coreTerms.put(DwcTerm.datasetName.qualifiedName(), "East Africa Mammal Survey");

    er.setCoreTerms(coreTerms);

    // Write to parquet
    Schema schema = ReflectData.AllowNull.get().getSchema(ExtendedRecord.class);

    // Output Parquet file path
    String outputPath = basePath + "/" + uuid + "/" + attempt + "/verbatim/verbatim.parquet";
    Path path = new Path(outputPath);

    try (ParquetWriter<ExtendedRecord> writer =
        AvroParquetWriter.<ExtendedRecord>builder(path)
            .withSchema(schema)
            .withDataModel(ReflectData.get())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .withWriteMode(OVERWRITE)
            .build()) {
      writer.write(er);
    }
  }

  private static final Map<String, Object> EXPECTED_HDFS_FIELDS =
      Map.ofEntries(
          // occurrence core
          Map.entry("basisofrecord", "HUMAN_OBSERVATION"),
          Map.entry("occurrenceid", "occ-123456"),
          Map.entry("scientificname", "Panthera leo (Linnaeus, 1758)"),
          Map.entry("taxonrank", "SPECIES"),
          Map.entry("taxonomicstatus", "ACCEPTED"),

          // taxonomy
          Map.entry("kingdom", "Animalia"),
          Map.entry("phylum", "Chordata"),
          Map.entry("class_", "Mammalia"),
          Map.entry("order", "Carnivora"),
          Map.entry("family", "Felidae"),
          Map.entry("genus", "Panthera"),
          Map.entry("species", "Panthera leo"),

          // location
          Map.entry("countrycode", "KE"),
          Map.entry("stateprovince", "Narok"),
          Map.entry("locality", "Maasai Mara National Reserve"),
          Map.entry("decimallatitude", -1.4061),
          Map.entry("decimallongitude", 35.0128),
          Map.entry("hascoordinate", true),

          // event
          Map.entry("eventdate", "2023"),
          Map.entry("year", 2023),

          // identifiers & counts
          Map.entry("individualcount", 3),
          Map.entry("gbifid", "1"),
          Map.entry("datasetkey", "7683cc47-cb13-4bad-9614-387c66aa8df0"),

          // institution & metadata
          Map.entry("institutioncode", "NMK"),
          Map.entry("license", "CC_BY_NC_4_0"),
          Map.entry("publishingcountry", "BG"));
}
