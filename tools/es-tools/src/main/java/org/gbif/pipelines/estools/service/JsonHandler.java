package org.gbif.pipelines.estools.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;

/**
 * Handler to work with JSON.
 *
 * <p>This class handles all the exceptions thrown when working with JSON and rethrows the checked
 * exceptions as unchecked.
 */
final class JsonHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();
  private static final ObjectReader READER = MAPPER.readerFor(Map.class);

  private JsonHandler() {}

  /** Creates a {@link ObjectNode}. */
  static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /** Creates a {@link ArrayNode}. */
  static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /** Writes a {@link Object} to String. */
  static String writeToString(Object obj) {
    try {
      return WRITER.writeValueAsString(obj);
    } catch (JsonProcessingException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Writes a {@link InputStream} to String . */
  static String writeToString(InputStream inputStream) {
    try {
      return writeToString(READER.readTree(inputStream));
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link Map}. */
  static Map<String, String> readValue(HttpEntity entity) {
    Objects.requireNonNull(entity);
    try {
      return READER.readValue(entity.getContent());
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link JsonNode}. */
  static JsonNode readTree(HttpEntity entity) {
    Objects.requireNonNull(entity);
    try {
      return READER.readTree(entity.getContent());
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Reads a {@link InputStream} with JSON content and returns it as a {@link JsonNode}. */
  static JsonNode readTree(InputStream inputStream) {
    try {
      return READER.readTree(inputStream);
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Reads a {@link String} with JSON content and returns it as a {@link JsonNode}. */
  static JsonNode readTree(String jsonString) {
    try {
      return READER.readTree(jsonString);
    } catch (IOException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /** Converts a {@link Map} into a {@link JsonNode}. */
  static JsonNode convertToJsonNode(Map<String, String> map) {
    return MAPPER.valueToTree(map);
  }
}
