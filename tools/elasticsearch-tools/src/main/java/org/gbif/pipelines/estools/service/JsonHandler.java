package org.gbif.pipelines.estools.service;

import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpEntity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 * Handler to work with JSON.
 *
 * <p>This class handles all the exceptions thrown when working with JSON and rethrows the checked
 * exceptions as unchecked.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class JsonHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();
  private static final ObjectReader READER = MAPPER.readerFor(Map.class);

  /** Creates a {@link ObjectNode}. */
  static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /** Creates a {@link ArrayNode}. */
  static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /** Writes a {@link Object} to String. */
  @SneakyThrows
  static String writeToString(Object obj) {
    return WRITER.writeValueAsString(obj);
  }

  /** Writes a {@link InputStream} to String . */
  @SneakyThrows
  static String writeToString(InputStream inputStream) {
    return writeToString(READER.readTree(inputStream));
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link Map}. */
  @SneakyThrows
  static Map<String, String> readValue(@NonNull HttpEntity entity) {
    return READER.readValue(entity.getContent());
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  static JsonNode readTree(@NonNull HttpEntity entity) {
    return READER.readTree(entity.getContent());
  }

  /** Reads a {@link InputStream} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  static JsonNode readTree(InputStream inputStream) {
    return READER.readTree(inputStream);
  }

  /** Reads a {@link String} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  static JsonNode readTree(String jsonString) {
    return READER.readTree(jsonString);
  }

  /** Converts a {@link Map} into a {@link JsonNode}. */
  static JsonNode convertToJsonNode(Map<String, String> map) {
    return MAPPER.valueToTree(map);
  }
}
