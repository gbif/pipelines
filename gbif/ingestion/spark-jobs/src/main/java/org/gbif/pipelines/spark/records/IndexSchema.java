package org.gbif.pipelines.spark.records;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Reads the Elasticsearch index schemas. Once records are served from HBase, the indices can have
 * the _source disabled ({@code indexConfig.sourceEnabled: false}), so they only search and return
 * document ids.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexSchema {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Mappings of a new index: the schema as is when the _source is enabled, otherwise the schema
   * with the _source disabled
   */
  public static String mappings(String schemaPath, boolean sourceEnabled) {
    JsonNode schema = read(schemaPath);
    if (!sourceEnabled) {
      ObjectNode source = MAPPER.createObjectNode().put("enabled", false);
      ((ObjectNode) schema).set("_source", source);
    }
    return schema.toString();
  }

  /**
   * Fields not sent to Elasticsearch: with the _source disabled, the {@link #unindexedFields}
   * aren't stored anywhere in the index
   */
  public static String[] fieldsNotSent(String schemaPath, boolean sourceEnabled) {
    return sourceEnabled ? new String[0] : unindexedFields(schemaPath).toArray(new String[0]);
  }

  /**
   * Top-level fields mapped with {@code "enabled": false}: they are neither indexed nor have doc
   * values, they are only kept in the _source.
   */
  public static List<String> unindexedFields(String schemaPath) {
    JsonNode schema = read(schemaPath);

    List<String> fields = new ArrayList<>();
    schema
        .path("properties")
        .fields()
        .forEachRemaining(
            field -> {
              JsonNode enabled = field.getValue().get("enabled");
              if (enabled != null && !enabled.asBoolean()) {
                fields.add(field.getKey());
              }
            });
    return fields;
  }

  private static JsonNode read(String schemaPath) {
    try (InputStream in = open(schemaPath)) {
      return MAPPER.readTree(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Can't read index schema " + schemaPath, e);
    }
  }

  /**
   * The schema is a file when the path is absolute, otherwise a classpath resource, as when the
   * index is created ({@code HttpRequestBuilder.loadFile})
   */
  private static InputStream open(String schemaPath) throws IOException {
    Path path = Paths.get(schemaPath);
    if (path.isAbsolute()) {
      return Files.newInputStream(path);
    }
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaPath);
    if (in == null) {
      throw new FileNotFoundException("Index schema not found on the classpath: " + schemaPath);
    }
    return in;
  }
}
