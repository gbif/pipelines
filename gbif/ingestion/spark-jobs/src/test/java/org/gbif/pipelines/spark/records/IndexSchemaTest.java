package org.gbif.pipelines.spark.records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class IndexSchemaTest {

  private static final String OCCURRENCE_SCHEMA = schemaPath("es-occurrence-schema.json");
  private static final String EVENT_SCHEMA = schemaPath("es-event-schema.json");

  /** Records are served from HBase, the indices only return document ids */
  @Test
  public void sourceIsDisabled() throws Exception {
    for (String schemaPath : List.of(OCCURRENCE_SCHEMA, EVENT_SCHEMA)) {
      JsonNode schema = new ObjectMapper().readTree(Files.readString(Paths.get(schemaPath)));
      assertFalse(schema.path("_source").path("enabled").asBoolean(true));
    }
  }

  @Test
  public void unindexedFields() {
    assertEquals(
        List.of("multimediaItems", "verbatim"), IndexSchema.unindexedFields(OCCURRENCE_SCHEMA));
    assertEquals(List.of("verbatim"), IndexSchema.unindexedFields(EVENT_SCHEMA));
  }

  private static String schemaPath(String name) {
    return IndexSchemaTest.class.getResource("/elasticsearch/" + name).getFile();
  }
}
