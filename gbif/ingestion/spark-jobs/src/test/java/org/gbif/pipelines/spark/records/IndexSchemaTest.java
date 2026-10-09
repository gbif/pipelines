package org.gbif.pipelines.spark.records;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.gbif.pipelines.core.config.model.IndexConfig;
import org.junit.Test;

public class IndexSchemaTest {

  private static final String OCCURRENCE_SCHEMA = schemaPath("es-occurrence-schema.json");
  private static final String EVENT_SCHEMA = schemaPath("es-event-schema.json");

  /** Until records are served from HBase, the indices are created with the schema as is */
  @Test
  public void sourceEnabledKeepsTheSchema() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    for (String schemaPath : List.of(OCCURRENCE_SCHEMA, EVENT_SCHEMA)) {
      JsonNode schema = mapper.readTree(Files.readString(Paths.get(schemaPath)));
      assertEquals(schema, mapper.readTree(IndexSchema.mappings(schemaPath, true)));
      assertArrayEquals(new String[0], IndexSchema.fieldsNotSent(schemaPath, true));
    }
  }

  /** Records are served from HBase, the indices only return document ids */
  @Test
  public void sourceDisabled() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    for (String schemaPath : List.of(OCCURRENCE_SCHEMA, EVENT_SCHEMA)) {
      JsonNode mappings = mapper.readTree(IndexSchema.mappings(schemaPath, false));
      assertEquals(mapper.readTree("{\"enabled\":false}"), mappings.path("_source"));
      assertFalse(mappings.path("properties").isMissingNode());
    }
    assertArrayEquals(
        new String[] {"multimediaItems", "verbatim"},
        IndexSchema.fieldsNotSent(OCCURRENCE_SCHEMA, false));
    assertArrayEquals(new String[] {"verbatim"}, IndexSchema.fieldsNotSent(EVENT_SCHEMA, false));
  }

  @Test
  public void unindexedFields() {
    assertEquals(
        List.of("multimediaItems", "verbatim"), IndexSchema.unindexedFields(OCCURRENCE_SCHEMA));
    assertEquals(List.of("verbatim"), IndexSchema.unindexedFields(EVENT_SCHEMA));
  }

  /** The default paths of the configuration are classpath resources, as with spark-submit */
  @Test
  public void defaultSchemasAreReadFromTheClasspath() {
    IndexConfig defaults = new IndexConfig();
    assertEquals(
        List.of("multimediaItems", "verbatim"),
        IndexSchema.unindexedFields(defaults.getOccurrenceSchemaPath()));
    assertEquals(List.of("verbatim"), IndexSchema.unindexedFields(defaults.getEventSchemaPath()));
  }

  private static String schemaPath(String name) {
    return IndexSchemaTest.class.getResource("/elasticsearch/" + name).getFile();
  }
}
