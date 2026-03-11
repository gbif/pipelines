package org.gbif.pipelines.spark;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.gbif.pipelines.ConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.SparkUtil.getFileSystem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.junit.*;

public class OccurrenceInterpretationTest extends MockedServicesTest {

  private static SparkSession spark;
  private static java.nio.file.Path sparkTmp = null;

  @BeforeClass
  public static void setUp() throws Exception {
    sparkTmp = Files.createTempDirectory("spark-local");
    spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.local.dir", sparkTmp.toAbsolutePath().toString())
            .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

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

    PipelinesConfig config = loadConfig(testResourcesRoot + "/pipelines.yaml");

    /* ############ standard init block ########## */
    FileSystem fileSystem = getFileSystem(spark, config);
    OccurrenceInterpretation.runInterpretation(
        spark, fileSystem, config, testUuid, attempt, 1, false, true, null);

    // check outputs exist
    assertParquetExists(basePath, testUuid, attempt, Directories.OCCURRENCE_HDFS);
    assertParquetExists(basePath, testUuid, attempt, Directories.OCCURRENCE_JSON);
    assertParquetExists(basePath, testUuid, attempt, Directories.SIMPLE_OCCURRENCE);
    assertParquetExists(basePath, testUuid, attempt, Directories.VERBATIM_EXT_FILTERED);

    // check hdfs outputs in detail
    checkHdfsTableOutputs(basePath, testUuid, attempt);
    checkJsonOutputs(basePath, testUuid, attempt);
  }

  private void checkJsonOutputs(String outputFile, String testUuid, int attempt) throws Exception {
    java.nio.file.Path dir =
        java.nio.file.Path.of(outputFile)
            .resolve(testUuid)
            .resolve(String.valueOf(attempt))
            .resolve(Directories.OCCURRENCE_JSON);

    java.nio.file.Path parquetFile =
        Files.list(dir)
            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
            .findFirst()
            .get();

    Configuration conf = new Configuration();

    Dataset<OccurrenceJsonRecord> jsonDf =
        spark.read().parquet(parquetFile.toString()).as(Encoders.bean(OccurrenceJsonRecord.class));
    List<String> jsonRecord = jsonDf.toJSON().collectAsList();

    for (String json : jsonRecord) {

      Integer eventId = JsonPath.read(json, "$.gbifId");
      // write json in pretty print to file for debugging
      java.nio.file.Files.write(
          java.nio.file.Path.of(outputFile + "occurrence-" + eventId + ".json"),
          new ObjectMapper().readTree(json).toPrettyString().getBytes(StandardCharsets.UTF_8));

      // JsonPath assertions
      assertEquals(
          List.of(
              "East Africa Mammal Survey",
              "Animalia",
              "Panthera leo",
              "Chordata",
              "A. Taxonomist",
              "Maasai Mara National Reserve",
              "leo",
              "KE",
              "Felidae",
              "30",
              "96",
              "Panthera",
              "Carnivora",
              "Narok",
              "14",
              "2023",
              "Kenya",
              "Mammals",
              "NMK",
              "occ-123456",
              "J. Smith",
              "Mammalia",
              "3",
              "event-98765",
              "HumanObservation",
              "species",
              "2023-01-01",
              "7",
              "-1.4061",
              "adult",
              "male",
              "35.0128"),
          JsonPath.<List<String>>read(json, "$.all"));

      assertEquals(0, JsonPath.<List<?>>read(json, "$.associatedSequences").size());
      assertEquals("HUMAN_OBSERVATION", JsonPath.read(json, "$.basisOfRecord"));
      assertEquals(
          List.of("d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"),
          JsonPath.<List<String>>read(json, "$.checklistKey"));

      assertEquals("Mammals", JsonPath.read(json, "$.collectionCode"));
      assertEquals("AFRICA", JsonPath.read(json, "$.continent"));
      assertEquals(30.0d, JsonPath.<Double>read(json, "$.coordinateUncertaintyInMeters"), 1e-6);
      assertEquals(-1.4061d, JsonPath.<Double>read(json, "$.coordinates.lat"), 1e-6);
      assertEquals(35.0128d, JsonPath.<Double>read(json, "$.coordinates.lon"), 1e-6);
      assertEquals("Kenya", JsonPath.read(json, "$.country"));
      assertEquals("KE", JsonPath.read(json, "$.countryCode"));
      assertEquals(1, JsonPath.<Integer>read(json, "$.crawlId").intValue());
      assertEquals("7683cc47-cb13-4bad-9614-387c66aa8df0", JsonPath.read(json, "$.datasetKey"));
      assertEquals(
          List.of("East Africa Mammal Survey"), JsonPath.<List<String>>read(json, "$.datasetName"));
      assertEquals("BG", JsonPath.read(json, "$.datasetPublishingCountry"));
      assertEquals(
          "Mid-winter count of water birds and other bird species in Bulgaria",
          JsonPath.read(json, "$.datasetTitle"));
      assertEquals(
          "8dae0f8c-12bc-444d-b889-6b177550a8b2", JsonPath.read(json, "$.endorsingNodeKey"));

      assertEquals("2023-01-01T00:00:00.000", JsonPath.read(json, "$.eventDate.gte"));
      assertEquals("2023-12-31T23:59:59.999", JsonPath.read(json, "$.eventDate.lte"));
      assertEquals("2023", JsonPath.read(json, "$.eventDateInterval"));
      assertEquals("2023-01-01T00:00", JsonPath.read(json, "$.eventDateSingle"));
      assertEquals("event-98765", JsonPath.read(json, "$.eventId"));
      assertEquals(
          List.of("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"),
          JsonPath.<List<String>>read(json, "$.extensions"));

      assertEquals(
          List.of("KEN", "KEN.33_1", "KEN.33.2_1", "KEN.33.2.4_1"),
          JsonPath.<List<String>>read(json, "$.gadm.gids"));
      assertEquals("KEN", JsonPath.read(json, "$.gadm.level0Gid"));
      assertEquals("Kenya", JsonPath.read(json, "$.gadm.level0Name"));
      assertEquals("KEN.33_1", JsonPath.read(json, "$.gadm.level1Gid"));
      assertEquals("Narok", JsonPath.read(json, "$.gadm.level1Name"));
      assertEquals("KEN.33.2_1", JsonPath.read(json, "$.gadm.level2Gid"));
      assertEquals("Kilgoris", JsonPath.read(json, "$.gadm.level2Name"));
      assertEquals("KEN.33.2.4_1", JsonPath.read(json, "$.gadm.level3Gid"));
      assertEquals("Kimintet", JsonPath.read(json, "$.gadm.level3Name"));

      assertEquals(1, JsonPath.<Integer>read(json, "$.gbifId").intValue());
      assertEquals("AFRICA", JsonPath.read(json, "$.gbifRegion"));
      assertEquals(0, JsonPath.<List<?>>read(json, "$.geologicalContext.biostratigraphy").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.geologicalContext.lithostratigraphy").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.georeferencedBy").size());
      assertEquals(true, JsonPath.<Boolean>read(json, "$.hasCoordinate"));
      assertEquals(false, JsonPath.<Boolean>read(json, "$.hasGeospatialIssue"));
      assertEquals(0, JsonPath.<List<?>>read(json, "$.higherGeography").size());
      assertEquals(
          "fbca90e3-8aed-48b1-84e3-369afbd000ce", JsonPath.read(json, "$.hostingOrganizationKey"));
      assertEquals("1", JsonPath.read(json, "$.id"));
      assertEquals(List.of("A. Taxonomist"), JsonPath.<List<String>>read(json, "$.identifiedBy"));
      assertEquals(List.of(), JsonPath.<List<String>>read(json, "$.identifiedByIds"));
      assertEquals("A. Taxonomist", JsonPath.read(json, "$.identifiedByJoined"));
      assertEquals(3, JsonPath.<Integer>read(json, "$.individualCount").intValue());
      assertEquals(
          "17a83780-3060-4851-9d6f-029d5fcb81c9", JsonPath.read(json, "$.installationKey"));
      assertEquals("NMK", JsonPath.read(json, "$.institutionCode"));
      assertEquals(false, JsonPath.<Boolean>read(json, "$.isClustered"));
      assertEquals(false, JsonPath.<Boolean>read(json, "$.isSequenced"));

      assertEquals(
          List.of(
              "OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT",
              "RECORDED_DATE_MISMATCH",
              "GEODETIC_DATUM_ASSUMED_WGS84",
              "CONTINENT_DERIVED_FROM_COORDINATES"),
          JsonPath.<List<String>>read(json, "$.issues"));

      assertEquals("2026-01-30T10:01:03.422", JsonPath.read(json, "$.lastCrawled"));
      assertEquals("CC_BY_NC_4_0", JsonPath.read(json, "$.license"));
      assertEquals("Adult", JsonPath.read(json, "$.lifeStage.concept"));
      assertEquals(List.of("Adult"), JsonPath.<List<String>>read(json, "$.lifeStage.lineage"));
      assertEquals("Maasai Mara National Reserve", JsonPath.read(json, "$.locality"));
      assertEquals(List.of("wing length"), JsonPath.<List<String>>read(json, "$.measurementTypes"));
      assertEquals(0, JsonPath.<List<?>>read(json, "$.mediaLicenses").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.mediaTypes").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.multimediaItems").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.networkKeys").size());

      assertEquals(
          List.of(
              "OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT",
              "RECORDED_DATE_MISMATCH",
              "GEODETIC_DATUM_ASSUMED_WGS84",
              "CONTINENT_DERIVED_FROM_COORDINATES"),
          JsonPath.<List<String>>read(json, "$.nonTaxonomicIssues"));

      assertEquals("occ-123456", JsonPath.read(json, "$.occurrenceId"));
      assertEquals("PRESENT", JsonPath.read(json, "$.occurrenceStatus"));
      assertEquals(0, JsonPath.<List<?>>read(json, "$.otherCatalogNumbers").size());
      assertEquals(0, JsonPath.<List<?>>read(json, "$.preparations").size());
      assertEquals(List.of("nlbif2022.015"), JsonPath.<List<String>>read(json, "$.projectId"));
      assertEquals("nlbif2022.015", JsonPath.read(json, "$.projectIdJoined"));
      assertEquals("EML", JsonPath.read(json, "$.protocol"));
      assertEquals("EUROPE", JsonPath.read(json, "$.publishedByGbifRegion"));
      assertEquals(
          "Bulgarian Society for the Protection of Birds", JsonPath.read(json, "$.publisherTitle"));
      assertEquals("BG", JsonPath.read(json, "$.publishingCountry"));
      assertEquals(
          "d1fec91e-5b53-4cff-9b31-10fed530a3c3",
          JsonPath.read(json, "$.publishingOrganizationKey"));
      assertEquals(List.of("J. Smith"), JsonPath.<List<String>>read(json, "$.recordedBy"));
      assertEquals(List.of(), JsonPath.<List<String>>read(json, "$.recordedByIds"));
      assertEquals("J. Smith", JsonPath.read(json, "$.recordedByJoined"));
      assertEquals(true, JsonPath.<Boolean>read(json, "$.repatriated"));
      assertEquals(0, JsonPath.<List<?>>read(json, "$.samplingProtocol").size());
      assertEquals("POINT (35.0128 -1.4061)", JsonPath.read(json, "$.scoordinates"));
      assertEquals("Narok", JsonPath.read(json, "$.stateProvince"));

      // verbatim core assertions
      assertEquals(
          "event-98765",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventID']"));
      assertEquals(
          "Kenya", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/country']"));
      assertEquals(
          "Mammals",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/collectionCode']"));
      assertEquals(
          "Carnivora",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/order']"));
      assertEquals(
          "species",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/taxonRank']"));
      assertEquals(
          "2023", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/year']"));
      assertEquals(
          "adult",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/lifeStage']"));
      assertEquals(
          "Maasai Mara National Reserve",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/locality']"));
      assertEquals(
          "HumanObservation",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/basisOfRecord']"));
      assertEquals(
          "Felidae", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/family']"));
      assertEquals(
          "male", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/sex']"));
      assertEquals(
          "7", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/month']"));
      assertEquals(
          "-1.4061",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/decimalLatitude']"));
      assertEquals(
          "Panthera leo",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/scientificName']"));
      assertEquals(
          "Narok",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/stateProvince']"));
      assertEquals(
          "J. Smith",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/recordedBy']"));
      assertEquals(
          "Chordata",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/phylum']"));
      assertEquals(
          "14", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/day']"));
      assertEquals(
          "East Africa Mammal Survey",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/datasetName']"));
      assertEquals(
          "Animalia",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/kingdom']"));
      assertEquals(
          "Panthera", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/genus']"));
      assertEquals(
          "30",
          JsonPath.read(
              json,
              "$.verbatim.core['http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters']"));
      assertEquals(
          "96", JsonPath.read(json, "$.verbatim.core['http://unknown.org/crawl_attempt']"));
      assertEquals(
          "Mammalia", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/class']"));
      assertEquals(
          "3",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/individualCount']"));
      assertEquals(
          "leo",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/specificEpithet']"));
      assertEquals(
          "occ-123456",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/occurrenceID']"));
      assertEquals(
          "A. Taxonomist",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/identifiedBy']"));
      assertEquals(
          "2023-01-01",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/eventDate']"));
      assertEquals(
          "35.0128",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/decimalLongitude']"));
      assertEquals(
          "KE", JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/countryCode']"));
      assertEquals(
          "NMK",
          JsonPath.read(json, "$.verbatim.core['http://rs.tdwg.org/dwc/terms/institutionCode']"));
      assertEquals("1", JsonPath.read(json, "$.verbatim.coreId"));

      // verbatim MeasurementOrFact extension assertions
      assertEquals(
          "120",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementValue']"));
      assertEquals(
          "mm",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementUnit']"));
      assertEquals(
          "mof-001",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementID']"));
      assertEquals(
          "wing length",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementType']"));
      assertEquals(
          "2023-01-02",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementDeterminedDate']"));
      assertEquals(
          "calliper",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementMethod']"));
      assertEquals(
          "0.5",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementAccuracy']"));
      assertEquals(
          "J. Smith",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementDeterminedBy']"));
      assertEquals(
          "occ-123456",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/occurrenceID']"));
      assertEquals(
          "measured on right wing",
          JsonPath.read(
              json,
              "$.verbatim.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact'][0]['http://rs.tdwg.org/dwc/terms/measurementRemarks']"));

      assertEquals("Panthera leo", JsonPath.read(json, "$.verbatimScientificName"));
      assertEquals(2023, JsonPath.<Integer>read(json, "$.year").intValue());
      assertEquals(
          2245273112662769665L, JsonPath.<Long>read(json, "$.yearMonthGbifIdSort").longValue());

      // classifications assertions (by checklist key)
      String ck = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c";
      assertEquals(
          "5219404", JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.key"));
      assertEquals(
          "(Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.authorship"));
      assertEquals(
          "ZOOLOGICAL", JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.code"));
      assertEquals(
          "<i>Panthera</i> <i>leo</i> (Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.formattedName"));
      assertEquals(
          "Panthera",
          JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.genericName"));
      assertEquals(
          "Panthera leo (Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.name"));
      assertEquals(
          "SPECIES", JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.rank"));
      assertEquals(
          "leo",
          JsonPath.read(json, "$.classifications['" + ck + "'].acceptedUsage.specificEpithet"));

      assertEquals(
          "Chordata",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['PHYLUM']"));
      assertEquals(
          "Carnivora",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['ORDER']"));
      assertEquals(
          "Panthera",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['GENUS']"));
      assertEquals(
          "Panthera leo",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['SPECIES']"));
      assertEquals(
          "Animalia",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['KINGDOM']"));
      assertEquals(
          "Mammalia",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['CLASS']"));
      assertEquals(
          "Felidae",
          JsonPath.read(json, "$.classifications['" + ck + "'].classification['FAMILY']"));

      assertEquals(
          "1", JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['0']"));
      assertEquals(
          "44", JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['1']"));
      assertEquals(
          "359", JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['2']"));
      assertEquals(
          "732", JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['3']"));
      assertEquals(
          "9703", JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['4']"));
      assertEquals(
          "2435194",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['5']"));
      assertEquals(
          "5219404",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationDepth['6']"));

      assertEquals(
          "44",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['PHYLUM']"));
      assertEquals(
          "732",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['ORDER']"));
      assertEquals(
          "2435194",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['GENUS']"));
      assertEquals(
          "5219404",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['SPECIES']"));
      assertEquals(
          "1",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['KINGDOM']"));
      assertEquals(
          "359",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['CLASS']"));
      assertEquals(
          "9703",
          JsonPath.read(json, "$.classifications['" + ck + "'].classificationKeys['FAMILY']"));

      assertEquals(
          0, JsonPath.<List<?>>read(json, "$.classifications['" + ck + "'].issues").size());
      assertEquals(
          "VU", JsonPath.read(json, "$.classifications['" + ck + "'].iucnRedListCategoryCode"));
      assertEquals("ACCEPTED", JsonPath.read(json, "$.classifications['" + ck + "'].status"));
      assertEquals(
          List.of("1", "44", "359", "732", "9703", "2435194", "5219404"),
          JsonPath.<List<String>>read(json, "$.classifications['" + ck + "'].taxonKeys"));

      assertEquals(
          "Panthera leo (Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].usage.name"));
      assertEquals(
          "(Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].usage.authorship"));
      assertEquals("ZOOLOGICAL", JsonPath.read(json, "$.classifications['" + ck + "'].usage.code"));
      assertEquals(
          "<i>Panthera</i> <i>leo</i> (Linnaeus, 1758)",
          JsonPath.read(json, "$.classifications['" + ck + "'].usage.formattedName"));
      assertEquals(
          "Panthera", JsonPath.read(json, "$.classifications['" + ck + "'].usage.genericName"));
      assertEquals("5219404", JsonPath.read(json, "$.classifications['" + ck + "'].usage.key"));
      assertEquals("SPECIES", JsonPath.read(json, "$.classifications['" + ck + "'].usage.rank"));
      assertEquals(
          "leo", JsonPath.read(json, "$.classifications['" + ck + "'].usage.specificEpithet"));
    }
  }

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

    // Add a MoF extension
    Map<String, String> mofTerms = new HashMap<>();
    mofTerms.put(DwcTerm.measurementID.qualifiedName(), "mof-001");
    mofTerms.put(DwcTerm.measurementType.qualifiedName(), "wing length");
    mofTerms.put(DwcTerm.measurementValue.qualifiedName(), "120");
    mofTerms.put(DwcTerm.measurementUnit.qualifiedName(), "mm");
    mofTerms.put(DwcTerm.measurementAccuracy.qualifiedName(), "0.5");
    mofTerms.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2023-01-02");
    mofTerms.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "J. Smith");
    mofTerms.put(DwcTerm.measurementMethod.qualifiedName(), "calliper");
    mofTerms.put(DwcTerm.measurementRemarks.qualifiedName(), "measured on right wing");
    mofTerms.put(DwcTerm.occurrenceID.qualifiedName(), "occ-123456");

    er.setExtensions(Map.of(DwcTerm.MeasurementOrFact.qualifiedName(), List.of(mofTerms)));

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

  private static final Map<String, Object> EXPECTED_JSON_FIELDS =
      Map.ofEntries(
          // occurrence core
          Map.entry("basisOfRecord", "HUMAN_OBSERVATION"),

          // location
          Map.entry("countryCode", "KE"),
          Map.entry("stateProvince", "Narok"),
          Map.entry("locality", "Maasai Mara National Reserve"),
          Map.entry("decimalLatitude", -1.4061),
          Map.entry("decimalLongitude", 35.0128),
          Map.entry("hasCoordinate", true),

          // event
          Map.entry(
              "eventDate",
              "{\"gte\": \"2023-01-01T00:00:00.000\", \"lte\": \"2023-12-31T23:59:59.999\"}"),
          Map.entry("year", 2023),

          // identifiers & counts
          Map.entry("individualCount", 3),
          Map.entry("gbifId", "1"),
          Map.entry("datasetKey", "7683cc47-cb13-4bad-9614-387c66aa8df0"),

          // institution & metadata
          Map.entry("institutionCode", "NMK"),
          Map.entry("license", "CC_BY_NC_4_0"),
          Map.entry("publishingCountry", "BG"));
}
