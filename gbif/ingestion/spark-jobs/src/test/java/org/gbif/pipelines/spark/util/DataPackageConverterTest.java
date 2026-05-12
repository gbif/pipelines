package org.gbif.pipelines.spark.util;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.descriptor.ResourceDescriptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataPackageConverterTest {

  SparkSession spark;
  DataPackageConverter converter;
  Path fixtures;

  @BeforeAll
  void setup() throws URISyntaxException {
    spark = SparkTestSession.createBuilder().appName("converter-test").getOrCreate();
    converter = new DataPackageConverter(new JacksonDataPackageParser(), new ObjectMapper());
    fixtures = Paths.get(getClass().getClassLoader().getResource("fixtures").toURI());
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void tsvPackageIsConvertedToParquet(@TempDir Path destination) throws Exception {
    converter.convert(spark, fixtures.resolve("tsv-package"), "file://" + destination);

    Dataset<Row> occ = spark.read().parquet("file://" + destination + "/occurrences.parquet");
    assertEquals(3, occ.count());
    assertEquals("Puma concolor", occ.filter("id = '1'").first().getAs("scientificName"));

    Dataset<Row> taxa = spark.read().parquet("file://" + destination + "/taxa.parquet");
    assertEquals(3, taxa.count());
  }

  @Test
  void csvPackageIsConvertedToParquet(@TempDir Path destination) throws Exception {
    converter.convert(spark, fixtures.resolve("csv-package"), "file://" + destination);

    Dataset<Row> df = spark.read().parquet("file://" + destination + "/occurrences.parquet");
    assertEquals(3, df.count());
    // if delimiter was wrong everything lands in one column
    assertTrue(df.columns().length > 1);
  }

  @Test
  void parquetPassthroughPreservesData(@TempDir Path destination) throws Exception {
    Path source = fixtures.resolve("parquet-package");
    converter.convert(spark, source, "file://" + destination);

    Dataset<Row> original = spark.read().parquet("file://" + source + "/occurrences.parquet");
    Dataset<Row> converted = spark.read().parquet("file://" + destination + "/occurrences.parquet");

    assertEquals(original.count(), converted.count());
    assertEquals(original.schema(), converted.schema());
  }

  @Test
  void mixedPackageHandlesBothFormats(@TempDir Path destination) throws Exception {
    converter.convert(spark, fixtures.resolve("mixed-package"), "file://" + destination);

    Dataset<Row> occ = spark.read().parquet("file://" + destination + "/occurrences.parquet");
    assertEquals(2, occ.count());

    Dataset<Row> media = spark.read().parquet("file://" + destination + "/media.parquet");
    assertEquals(2, media.count());
  }

  @Test
  void sourceCanBeDescriptorFileDirectly(@TempDir Path destination) throws Exception {
    converter.convert(
        spark, fixtures.resolve("tsv-package/datapackage.json"), "file://" + destination);

    Dataset<Row> df = spark.read().parquet("file://" + destination + "/occurrences.parquet");
    assertEquals(3, df.count());
  }

  @Test
  void outputDescriptorIsWritten(@TempDir Path destination) throws Exception {
    converter.convert(spark, fixtures.resolve("tsv-package"), "file://" + destination);

    DataPackageDescriptor out =
        new JacksonDataPackageParser().parse(destination.resolve("datapackage.json"));

    assertEquals(2, out.resources().size());

    ResourceDescriptor occ =
        out.resources().stream()
            .filter(r -> r.name().equals("occurrences"))
            .findFirst()
            .orElseThrow();

    assertEquals("occurrences.parquet", occ.paths().get(0).getFileName().toString());
    assertNull(occ.dialect());
    assertFalse(occ.fields().isEmpty());
  }

  @Test
  void outputDescriptorPreservesForeignKeys(@TempDir Path destination) throws Exception {
    converter.convert(spark, fixtures.resolve("mixed-package"), "file://" + destination);

    DataPackageDescriptor out =
        new JacksonDataPackageParser().parse(destination.resolve("datapackage.json"));

    ResourceDescriptor media =
        out.resources().stream().filter(r -> r.name().equals("media")).findFirst().orElseThrow();

    assertFalse(media.foreignKeys().isEmpty());
    assertEquals("occurrences", media.foreignKeys().get(0).reference().resource());
  }
}
