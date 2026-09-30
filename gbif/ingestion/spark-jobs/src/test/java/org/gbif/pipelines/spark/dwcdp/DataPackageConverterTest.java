package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.FieldDescriptor;
import org.gbif.dp.descriptor.MissingValueDescriptor;
import org.gbif.dp.descriptor.ResourceDescriptor;
import org.gbif.dp.descriptor.SchemaDescriptor;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataPackageConverterTest {

  private static final long TARGET_PARTITION_BYTES = 128L * 1024L * 1024L;

  private SparkSession spark;
  private ObjectMapper mapper;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("DataPackageConverterTest").getOrCreate();
    mapper = new ObjectMapper();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void csvConversionUsesDescriptorTypesAndFieldMissingValues(@TempDir Path dir) throws Exception {
    Path source = dir.resolve("source");
    Files.createDirectories(source.resolve("data"));
    Files.writeString(
        source.resolve("data/event.csv"),
        "id,count,weight,active,eventDate,observedAt,comment\n"
            + "a,12,3.5,true,2024-01-02,2024-01-02T03:04:05,first\n"
            + "b,0,null,false,null,null,second\n"
            + "c,not-an-int,not-a-number,true,2024-01-04,2024-01-04T05:06:07,third\n",
        StandardCharsets.UTF_8);

    ResourceDescriptor resource =
        resource(
            "event",
            "data/event.csv",
            field("id", "string"),
            field("count", "integer", "0"),
            field("weight", "number", "null"),
            field("active", "boolean"),
            field("eventDate", "date", "null"),
            field("observedAt", "datetime", "null"),
            field("comment", "string"));
    DataPackageDescriptor descriptor = dataPackage(resource);

    // Deliberately include metadata the converter does not know about. Rewriting the descriptor as
    // a JSON tree should preserve it rather than forcing the HDFS writer to understand the complete
    // descriptor model.
    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {
          "name": "typed-test",
          "title": "Typed conversion test",
          "x-custom-package-property": {"keep": true},
          "resources": [{
            "name": "event",
            "path": "data/event.csv",
            "format": "csv",
            "encoding": "utf-8",
            "bytes": 123,
            "hash": "stale-after-conversion",
            "dialect": {"delimiter": ","},
            "x-custom-resource-property": "also-keep",
            "schema": {
              "fields": [
                {"name":"id","type":"string"},
                {"name":"count","type":"integer","missingValues":["0"]},
                {"name":"weight","type":"number","missingValues":["null"]},
                {"name":"active","type":"boolean"},
                {"name":"eventDate","type":"date","missingValues":["null"]},
                {"name":"observedAt","type":"datetime","missingValues":["null"]},
                {"name":"comment","type":"string"}
              ]
            }
          }]
        }
        """,
        StandardCharsets.UTF_8);

    DataPackageConverter converter =
        new DataPackageConverter(content -> descriptor, mapper, TARGET_PARTITION_BYTES);
    String destination = dir.resolve("output").toUri().toString();

    converter.convert(spark, source, destination);

    Dataset<Row> converted =
        spark
            .read()
            .parquet(dir.resolve("output/datapackage/data/event.parquet").toUri().toString())
            .orderBy("id");

    assertEquals(DataTypes.StringType, converted.schema().apply("id").dataType());
    assertEquals(DataTypes.LongType, converted.schema().apply("count").dataType());
    assertEquals(DataTypes.DoubleType, converted.schema().apply("weight").dataType());
    assertEquals(DataTypes.BooleanType, converted.schema().apply("active").dataType());
    assertEquals(DataTypes.DateType, converted.schema().apply("eventDate").dataType());
    assertEquals(DataTypes.TimestampType, converted.schema().apply("observedAt").dataType());
    assertEquals(DataTypes.StringType, converted.schema().apply("comment").dataType());

    // Do not collect DateType directly into Java in this test. Older Spark versions use
    // sun.util.calendar.ZoneInfo while externalizing java.sql.Date, which is blocked by the Java
    // module system unless the test JVM adds an --add-exports flag. Projecting dates/timestamps to
    // strings keeps this test focused on converter semantics while the schema assertions above
    // still prove that Parquet physically stores DateType/TimestampType.
    Dataset<Row> values =
        converted
            .selectExpr(
                "id",
                "count",
                "weight",
                "active",
                "cast(eventDate as string) as eventDate",
                "cast(observedAt as string) as observedAt",
                "comment")
            .orderBy("id");

    List<Row> rows = values.collectAsList();
    assertEquals(3, rows.size());

    Row first = rows.get(0);
    assertEquals(12L, first.<Long>getAs("count"));
    assertEquals(3.5d, first.<Double>getAs("weight"));
    assertEquals(Boolean.TRUE, first.<Boolean>getAs("active"));
    assertEquals("2024-01-02", first.<String>getAs("eventDate"));
    assertTrue(first.<String>getAs("observedAt").startsWith("2024-01-02 03:04:05"));

    Row missing = rows.get(1);
    assertNull(missing.getAs("count"), "field missing value '0' must become null");
    assertNull(missing.getAs("weight"));
    assertNull(missing.getAs("eventDate"));
    assertNull(missing.getAs("observedAt"));

    Row invalid = rows.get(2);
    assertNull(
        invalid.getAs("count"), "unexpected invalid integer is permissively converted to null");
    assertNull(
        invalid.getAs("weight"), "unexpected invalid number is permissively converted to null");

    Path outputDescriptor = dir.resolve("output/datapackage/datapackage.json");
    JsonNode rewritten = mapper.readTree(Files.readString(outputDescriptor));
    JsonNode rewrittenResource = rewritten.path("resources").get(0);

    assertEquals("Typed conversion test", rewritten.path("title").asText());
    assertTrue(rewritten.path("x-custom-package-property").path("keep").asBoolean());
    assertEquals("also-keep", rewrittenResource.path("x-custom-resource-property").asText());
    assertEquals("data/event.parquet", rewrittenResource.path("path").asText());
    assertEquals("parquet", rewrittenResource.path("format").asText());
    assertTrue(rewrittenResource.path("dialect").isMissingNode());
    assertTrue(rewrittenResource.path("encoding").isMissingNode());
    assertTrue(rewrittenResource.path("bytes").isMissingNode());
    assertTrue(rewrittenResource.path("hash").isMissingNode());
    assertEquals(
        "0",
        rewrittenResource
            .path("schema")
            .path("fields")
            .get(1)
            .path("missingValues")
            .get(0)
            .asText());
  }

  @Test
  void conversionPreservesAssertionValueColumnNameInDescriptorAndParquet(@TempDir Path dir)
      throws Exception {
    Path source = dir.resolve("source");
    Files.createDirectories(source.resolve("data"));
    Files.writeString(
        source.resolve("data/material-assertion.csv"),
        "assertionID,materialEntity_fk,assertionType,assertionValue,assertionUnit\n"
            + "A1,M1,temperature,12.5,C\n",
        StandardCharsets.UTF_8);

    ResourceDescriptor resource =
        resource(
            "material-assertion",
            "data/material-assertion.csv",
            field("assertionID", "string"),
            field("materialEntity_fk", "string"),
            field("assertionType", "string"),
            field("assertionValue", "string"),
            field("assertionUnit", "string"));
    DataPackageDescriptor descriptor = dataPackage(resource);

    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {
          "name": "assertion-column-name-test",
          "resources": [{
            "name": "material-assertion",
            "path": "data/material-assertion.csv",
            "schema": {
              "fields": [
                {"name":"assertionID","type":"string"},
                {"name":"materialEntity_fk","type":"string"},
                {"name":"assertionType","type":"string"},
                {"name":"assertionValue","type":"string"},
                {"name":"assertionUnit","type":"string"}
              ]
            }
          }]
        }
        """,
        StandardCharsets.UTF_8);

    DataPackageConverter converter =
        new DataPackageConverter(content -> descriptor, mapper, TARGET_PARTITION_BYTES);
    converter.convert(spark, source, dir.resolve("output").toUri().toString());

    Dataset<Row> converted =
        spark
            .read()
            .parquet(
                dir.resolve("output/datapackage/data/material-assertion.parquet")
                    .toUri()
                    .toString());

    List<String> parquetColumns = List.of(converted.columns());
    assertTrue(parquetColumns.contains("assertionValue"));
    assertFalse(parquetColumns.contains("assertionValueNumeric"));
    assertEquals(DataTypes.StringType, converted.schema().apply("assertionValue").dataType());
    assertEquals("12.5", converted.select("assertionValue").first().getString(0));

    JsonNode rewritten =
        mapper.readTree(Files.readString(dir.resolve("output/datapackage/datapackage.json")));
    JsonNode fields = rewritten.path("resources").get(0).path("schema").path("fields");
    List<String> descriptorFieldNames = new java.util.ArrayList<>();
    fields.forEach(field -> descriptorFieldNames.add(field.path("name").asText()));

    assertTrue(descriptorFieldNames.contains("assertionValue"));
    assertFalse(descriptorFieldNames.contains("assertionValueNumeric"));
  }

  @Test
  void parquetInputIsNormalizedToDescriptorSchema(@TempDir Path dir) throws Exception {
    Path source = dir.resolve("source");
    Files.createDirectories(source.resolve("data"));

    spark
        .range(1, 3)
        .withColumnRenamed("id", "count")
        .write()
        .parquet(source.resolve("data/event.parquet").toUri().toString());

    ResourceDescriptor resource = resource("event", "data/event.parquet", field("count", "string"));
    DataPackageDescriptor descriptor = dataPackage(resource);
    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {"name":"parquet-test","resources":[{"name":"event","path":"data/event.parquet","schema":{"fields":[{"name":"count","type":"string"}]}}]}
        """,
        StandardCharsets.UTF_8);

    DataPackageConverter converter =
        new DataPackageConverter(content -> descriptor, mapper, TARGET_PARTITION_BYTES);
    String destination = dir.resolve("output").toUri().toString();
    converter.convert(spark, source, destination);

    Dataset<Row> converted =
        spark
            .read()
            .parquet(dir.resolve("output/datapackage/data/event.parquet").toUri().toString());
    assertEquals(
        DataTypes.StringType,
        converted.schema().apply("count").dataType(),
        "The Data Package descriptor should be authoritative even for Parquet input");
    assertEquals(
        List.of("1", "2"),
        converted.orderBy("count").collectAsList().stream()
            .map(row -> row.<String>getAs("count"))
            .toList());
  }

  @Test
  void typeMappingKeepsUnsupportedFrictionlessTypesLexical() {
    assertEquals(DataTypes.LongType, DataPackageConverter.toSparkType(field("v", "integer")));
    assertEquals(DataTypes.DoubleType, DataPackageConverter.toSparkType(field("v", "number")));
    assertEquals(DataTypes.BooleanType, DataPackageConverter.toSparkType(field("v", "boolean")));
    assertEquals(DataTypes.DateType, DataPackageConverter.toSparkType(field("v", "date")));
    assertEquals(DataTypes.TimestampType, DataPackageConverter.toSparkType(field("v", "datetime")));
    assertEquals(DataTypes.IntegerType, DataPackageConverter.toSparkType(field("v", "year")));
    assertEquals(DataTypes.StringType, DataPackageConverter.toSparkType(field("v", "time")));
    assertEquals(DataTypes.StringType, DataPackageConverter.toSparkType(field("v", "geojson")));
  }

  private static DataPackageDescriptor dataPackage(ResourceDescriptor... resources) {
    return DataPackageDescriptor.builder().name("test").resources(List.of(resources)).build();
  }

  private static ResourceDescriptor resource(String name, String path, FieldDescriptor... fields) {
    return ResourceDescriptor.builder()
        .name(name)
        .paths(List.of(path))
        .schema(SchemaDescriptor.builder().fields(List.of(fields)).build())
        .build();
  }

  private static FieldDescriptor field(String name, String type, String... missingValues) {
    FieldDescriptor.Builder builder = FieldDescriptor.builder().name(name).type(type);
    if (missingValues.length > 0) {
      builder.missingValues(
          java.util.Arrays.stream(missingValues)
              .map(value -> new MissingValueDescriptor(value, MissingValueDescriptor.Source.FIELD))
              .toList());
    }
    return builder.build();
  }
}
