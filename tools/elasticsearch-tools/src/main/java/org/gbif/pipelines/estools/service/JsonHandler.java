package org.gbif.pipelines.estools.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;

/**
 * Handler to work with JSON.
 *
 * <p>This class handles all the exceptions thrown when working with JSON and rethrows the checked
 * exceptions as unchecked.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(Map.class);

  // Utility predicates to check if a node is a complex element
  private static final Predicate<String> IS_OBJECT =
      value -> value.startsWith("{\"") && value.endsWith("}");
  private static final Predicate<String> IS_ARRAY_ONE =
      value -> value.startsWith("[") && value.endsWith("]");
  private static final Predicate<String> IS_ARRAY_TWO =
      value -> value.startsWith("[{") && value.endsWith("}]");
  private static final Predicate<String> IS_VALID_JSON =
      value -> {
        try (JsonParser parser = MAPPER.getFactory().createParser(value)) {
          while (parser.nextToken() != null) {
            // NOP
          }
        } catch (Exception ex) {
          log.warn("JSON is invalid - {}", value);
          return false;
        }
        return true;
      };

  private static final Predicate<String> IS_COMPLEX_OBJECT =
      IS_OBJECT.or(IS_ARRAY_ONE).or(IS_ARRAY_TWO).and(IS_VALID_JSON);

  /** Creates a {@link ObjectNode}. */
  public static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /** Creates a {@link ArrayNode}. */
  public static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /** Writes a {@link InputStream} to String . */
  @SneakyThrows
  public static String toString(InputStream inputStream) {
    return READER.readTree(inputStream).toString();
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link Map}. */
  @SneakyThrows
  public static Map<String, String> readValue(@NonNull HttpEntity entity) {
    return READER.readValue(entity.getContent());
  }

  /** Reads a {@link HttpEntity} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  public static JsonNode readTree(@NonNull HttpEntity entity) {
    return READER.readTree(entity.getContent());
  }

  /** Reads a {@link InputStream} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  public static JsonNode readTree(InputStream inputStream) {
    return READER.readTree(inputStream);
  }

  /** Reads a {@link String} with JSON content and returns it as a {@link JsonNode}. */
  @SneakyThrows
  public static JsonNode readTree(String jsonString) {
    return READER.readTree(jsonString);
  }

  /** Converts a {@link Map} into a {@link JsonNode}. */
  public static JsonNode convertToJsonNode(Map<String, String> map) {
    ObjectNode objectNode = MAPPER.createObjectNode();

    map.entrySet().stream()
        .filter(x -> !IS_COMPLEX_OBJECT.test(x.getValue()))
        .forEach(e -> objectNode.put(e.getKey(), e.getValue()));

    map.entrySet().stream()
        .filter(x -> IS_COMPLEX_OBJECT.test(x.getValue()))
        .forEach(e -> objectNode.putPOJO(e.getKey(), new POJONode(e.getValue())));

    return objectNode;
  }
}
