package org.gbif.pipelines.spark.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

/**
 * Guards the published Parquet schemas. Fails when {@code Occurrence} or {@code Event} changes
 * without a version bump and regenerated schema files, or when a minor version is not additive.
 */
public class ParquetSchemasTest {

  @Test
  public void currentSchemasArePublished() throws IOException {
    for (ParquetSchemas.Published schema : ParquetSchemas.Published.values()) {
      String resource = schema.resourcePath(schema.version, ParquetSchemas.PARQUET_SCHEMA_SUFFIX);
      String published = readResource(resource);
      assertNotNull(
          String.format(
              "No published %s schema for version %s. Run ParquetSchemaGenerator.",
              schema.schemaName, schema.version),
          published);
      assertEquals(
          String.format(
              "The %s POJO no longer matches published schema %s. Bump the version in"
                  + " ParquetSchemas (minor for new optional fields, major otherwise) and run"
                  + " ParquetSchemaGenerator.",
              schema.schemaName, schema.version),
          published,
          schema.parquetMessage());
    }
  }

  @Test
  public void versionsWithinAMajorAreAdditive() throws Exception {
    for (ParquetSchemas.Published schema : ParquetSchemas.Published.values()) {
      String major = schema.version.split("\\.")[0];
      StructType current = schema.sparkSchema();
      for (String version : publishedVersions(schema)) {
        if (!version.split("\\.")[0].equals(major) || version.equals(schema.version)) {
          continue;
        }
        String json =
            readResource(schema.resourcePath(version, ParquetSchemas.SPARK_SCHEMA_SUFFIX));
        assertNotNull(json);
        StructType older = (StructType) DataType.fromJson(json);
        List<String> problems = new ArrayList<>();
        compare(schema.schemaName, older, current, problems);
        assertTrue(
            String.format(
                "%s %s is not additive over %s: %s",
                schema.schemaName, schema.version, version, problems),
            problems.isEmpty());
      }
    }
  }

  @Test
  public void descriptorNamesSchemaAndVersion() throws IOException {
    JsonNode node =
        new ObjectMapper()
            .readTree(ParquetSchemas.descriptorJson(ParquetSchemas.Published.OCCURRENCE_SCHEMA));
    assertEquals("occurrence", node.get("name").asText());
    assertEquals(ParquetSchemas.OCCURRENCE_VERSION, node.get("version").asText());
    assertTrue(node.get("parquetSchema").asText().startsWith("message occurrence {"));
  }

  @Test
  public void transformOutputsAreNestedStructs() {
    StructType occurrence = ParquetSchemas.Published.OCCURRENCE_SCHEMA.sparkSchema();
    for (String field :
        new String[] {"verbatim", "identifier", "basic", "location", "taxon", "temporal"}) {
      assertTrue(field, occurrence.apply(field).dataType() instanceof StructType);
    }
    StructType event = ParquetSchemas.Published.EVENT_SCHEMA.sparkSchema();
    for (String field :
        new String[] {"eventCore", "location", "temporal", "eventInherited", "derivedMetadata"}) {
      assertTrue(field, event.apply(field).dataType() instanceof StructType);
    }
  }

  /** Every field of {@code older} must exist in {@code newer} with a compatible type. */
  private static void compare(String path, DataType older, DataType newer, List<String> problems) {
    if (older instanceof StructType) {
      if (!(newer instanceof StructType)) {
        problems.add(path + " is no longer a struct");
        return;
      }
      StructType n = (StructType) newer;
      for (StructField f : ((StructType) older).fields()) {
        if (n.getFieldIndex(f.name()).isEmpty()) {
          problems.add(path + "." + f.name() + " was removed");
        } else {
          compare(path + "." + f.name(), f.dataType(), n.apply(f.name()).dataType(), problems);
        }
      }
    } else if (older instanceof ArrayType) {
      if (!(newer instanceof ArrayType)) {
        problems.add(path + " is no longer an array");
        return;
      }
      compare(
          path + "[]",
          ((ArrayType) older).elementType(),
          ((ArrayType) newer).elementType(),
          problems);
    } else if (older instanceof MapType) {
      if (!(newer instanceof MapType)) {
        problems.add(path + " is no longer a map");
        return;
      }
      compare(path + "{}", ((MapType) older).valueType(), ((MapType) newer).valueType(), problems);
    } else if (!older.equals(newer)) {
      problems.add(path + " changed from " + older.simpleString() + " to " + newer.simpleString());
    }
  }

  private static List<String> publishedVersions(ParquetSchemas.Published schema)
      throws URISyntaxException, IOException {
    URL root =
        ParquetSchemasTest.class
            .getClassLoader()
            .getResource(ParquetSchemas.RESOURCE_ROOT + "/" + schema.schemaName);
    List<String> versions = new ArrayList<>();
    if (root == null) {
      return versions;
    }
    try (Stream<Path> dirs = Files.list(Paths.get(root.toURI()))) {
      dirs.filter(Files::isDirectory).forEach(d -> versions.add(d.getFileName().toString()));
    }
    return versions;
  }

  private static String readResource(String resource) throws IOException {
    try (InputStream in = ParquetSchemasTest.class.getClassLoader().getResourceAsStream(resource)) {
      return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
