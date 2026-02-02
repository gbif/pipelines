package org.gbif.pipelines.spark;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class OccurrenceInterpretationTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Rule
  public WireMockRule registryRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @Rule
  public WireMockRule nameMatchingRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @Rule
  public WireMockRule grsciCollRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @Rule
  public WireMockRule geocodeRule =
      new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  @Test
  public void test() throws Exception {

    URL testRootUrl = getClass().getResource("/");
    assert testRootUrl != null;
    String testResourcesRoot = testRootUrl.getFile();

    // setup hbase, registry, geocode, grscicoll, taxon web services mocks here
    nameMatchingRule.stubFor(
        get(anyUrl())
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/namematching-response.json")))));

    geocodeRule.stubFor(
        get(anyUrl())
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/geocode-response.json")))));

    grsciCollRule.stubFor(
        get(anyUrl())
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/grscioll-response.json")))));

    registryRule.stubFor(
        get(urlPathMatching("/v1/dataset/7683cc47-cb13-4bad-9614-387c66aa8df0"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/registry-response.json")))));

    registryRule.stubFor(
        get(urlPathMatching("/v1/dataset/7683cc47-cb13-4bad-9614-387c66aa8df0/networks"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[]")));

    registryRule.stubFor(
        get(urlPathMatching("/v1/organization/d1fec91e-5b53-4cff-9b31-10fed530a3c3"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/organization-response.json")))));

    registryRule.stubFor(
        get(urlPathMatching("/v1/installation/17a83780-3060-4851-9d6f-029d5fcb81c9"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        Files.readString(
                            java.nio.file.Path.of(
                                testResourcesRoot + "ws/installation-response.json")))));

    // change pipelines config YAML to URLs of the mocked services if needed
    String testUuid = "7683cc47-cb13-4bad-9614-387c66aa8df0";
    int attempt = 1;

    // Generate test parquet files
    generateIdentifierParquet(testUuid, attempt);
    generateVerbatimParquet(testUuid, attempt);

    String outputFile = testRootUrl.getFile();

    TestConfigUtil.createConfigYaml(
        HBASE_SERVER.getZkQuorum(),
        testResourcesRoot,
        registryRule.port(),
        nameMatchingRule.port(),
        geocodeRule.port(),
        grsciCollRule.port());

    OccurrenceInterpretation.main(
        new String[] {
          "--appName=occurrence-interpretation-test",
          "--datasetId=" + testUuid,
          "--attempt=" + attempt,
          "--tripletValid=false",
          "--occurrenceIdValid=true",
          "--config=" + outputFile + "/pipelines.yaml",
          "--master=local[*]",
          "--numberOfShards=1",
          "--useSystemExit=false"
        });

    // check outputs exist
    assertParquetExists(outputFile, testUuid, attempt, "hdfs");
    assertParquetExists(outputFile, testUuid, attempt, "json");
    assertParquetExists(outputFile, testUuid, attempt, "simple-occurrence");
    assertParquetExists(outputFile, testUuid, attempt, "verbatim_ext_filtered");

    // check hdfs outputs in detail
    checkHdfsTableOutputs(outputFile, testUuid, attempt);
    checkJsonOutputs(outputFile, testUuid, attempt);
  }

  private void checkJsonOutputs(String outputFile, String testUuid, int attempt) {}

  private void checkHdfsTableOutputs(String outputFile, String testUuid, int attempt)
      throws Exception {

    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve("hdfs");

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

  private void generateIdentifierParquet(String uuid, int attempt) throws IOException {

    IdentifierRecord ir = new IdentifierRecord();
    ir.setId("1");
    ir.setInternalId("1");
    ir.setUniqueKey("1");

    // Write to parquet
    Schema schema = ReflectData.AllowNull.get().getSchema(IdentifierRecord.class);

    // Output Parquet file path
    String outputFile = getClass().getResource("/").getFile();
    String identifiersValidPath =
        outputFile + "/" + uuid + "/" + attempt + "/identifiers_valid/identifers.parquet";
    String identifiersAbsentPath =
        outputFile + "/" + uuid + "/" + attempt + "/identifiers_absent/identifers.parquet";

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

  private void generateVerbatimParquet(String uuid, int attempt) throws IOException {
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
    String outputFile = getClass().getResource("/").getFile();
    String outputPath = outputFile + "/" + uuid + "/" + attempt + "/verbatim/verbatim.parquet";
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
