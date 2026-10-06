package org.gbif.pipelines.spark.records;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Reads the Elasticsearch index schemas. Records are served from HBase and the indices have the
 * _source disabled, so they only search and return document ids.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexSchema {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Top-level fields mapped with {@code "enabled": false}: they are neither indexed nor have doc
   * values, so they aren't sent to Elasticsearch.
   */
  public static List<String> unindexedFields(String schemaPath) {
    JsonNode schema;
    try (InputStream in = open(schemaPath)) {
      schema = MAPPER.readTree(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Can't read index schema " + schemaPath, e);
    }

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
