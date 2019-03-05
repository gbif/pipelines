package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.extern.slf4j.Slf4j;

/**
 * Common converter, to convert any {@link SpecificRecordBase} object to json string
 *
 * <pre>{@code
 * Usage example:
 *
 * BasicRecord basic = ...
 * String result = SpecificRecordBase.create(interRecord).buildJson();
 *
 * }</pre>
 */
@Slf4j
public class JsonConverter {

  static final ObjectMapper MAPPER = new ObjectMapper();

  // Utility predicates to check if a node is a complex element
  private static final Predicate<String> IS_OBJECT = value -> value.startsWith("{\"") && value.endsWith("}");
  private static final Predicate<String> IS_ARRAY_ONE = value -> value.startsWith("[\"") && value.endsWith("]");
  private static final Predicate<String> IS_ARRAY_TWO = value -> value.startsWith("[{") && value.endsWith("}]");
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

  private final ObjectNode mainNode = MAPPER.createObjectNode();

  private final Map<Class<? extends SpecificRecordBase>, Consumer<SpecificRecordBase>> convertersMap = new HashMap<>();
  private SpecificRecordBase[] bases;
  private Set<String> skipKeys = new HashSet<>();
  private List<Pattern> replaceKeys = Collections.emptyList();

  JsonConverter() {}

  public static JsonConverter create() {
    return new JsonConverter();
  }

  public static JsonConverter create(SpecificRecordBase... bases) {
    return new JsonConverter().setSpecificRecordBase(bases);
  }

  JsonConverter setSpecificRecordBase(SpecificRecordBase... bases) {
    this.bases = bases;
    return this;
  }

  JsonConverter setReplaceKeys(String... replaceKeys) {
    this.replaceKeys = Arrays.stream(replaceKeys).map(Pattern::compile).collect(Collectors.toList());
    return this;
  }

  /** Set keys, if you don't want to see them in json string */
  JsonConverter setSkipKeys(String... escapeKeys) {
    skipKeys.addAll(Arrays.asList(escapeKeys));
    return this;
  }

  /**
   * You want to use another way how to process a specific class, you can use your appender for this
   * object
   *
   * <pre>{@code
   * Example:
   *
   * BiConsumer<SpecificRecordBase, StringBuilder> func = (record, sb) -> {
   *       Map<String, String> terms = ((ExtendedRecord) record).getCoreTerms();
   *       String example = map.get("Example");
   *       sb.append("\"exampleKey\":\"").append(example).append("\",");
   *     };
   * }</pre>
   */
  JsonConverter addSpecificConverter(Class<? extends SpecificRecordBase> type, Consumer<SpecificRecordBase> consumer) {
    convertersMap.put(type, consumer);
    return this;
  }

  public ObjectNode buildJson() {
    for (SpecificRecordBase record : bases) {
      Consumer<SpecificRecordBase> consumer = convertersMap.get(record.getClass());
      if (consumer != null) {
        consumer.accept(record);
      } else {
        addCommonFields(record);
      }
    }
    return mainNode;
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  JsonConverter addCommonFields(SpecificRecordBase base) {
    base.getSchema().getFields().forEach(
        f -> Optional.ofNullable(base.get(f.pos())).map(Object::toString).ifPresent(r -> addJsonField(f.name(), r))
    );
    return this;
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  JsonConverter addCommonFields(String key, SpecificRecordBase base) {
    ObjectNode node = MAPPER.createObjectNode();
    base.getSchema().getFields().forEach(
        f -> Optional.ofNullable(base.get(f.pos()))
            .map(Object::toString)
            .ifPresent(r -> addJsonField(node, f.name(), r))
    );
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonObject(String key, ObjectNode... nodes) {
    ObjectNode node = MAPPER.createObjectNode();
    Arrays.stream(nodes).forEach(node::setAll);
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonComplexObject(String key, Map<String, String> fields) {
    ObjectNode node = MAPPER.createObjectNode();
    fields.forEach((k, v) -> addJsonField(node, k, v));
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonRawObject(String key, Map<String, String> fields) {
    ObjectNode node = MAPPER.createObjectNode();
    fields.forEach((k, v) -> addJsonRawField(node, k, v));
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonArray(String key, List<ObjectNode> values) {
    ArrayNode node = mainNode.putArray(key);
    node.addAll(values);
    return this;
  }

  /** Check field in skipKeys and convert - "key":"value" */
  JsonConverter addJsonField(ObjectNode node, String key, String value) {
    return skipKeys.contains(key) ? this : addJsonFieldNoCheck(node, key, value);
  }

  /** Check field in skipKeys and convert - "key":"value" */
  JsonConverter addJsonRawField(ObjectNode node, String key, String value) {
    return skipKeys.contains(key) ? this : addJsonRawFieldNoCheck(node, key, value);
  }

  /** Check field in skipKeys and convert - "key":"value" */
  JsonConverter addJsonField(String key, String value) {
    return addJsonField(mainNode, key, value);
  }

  /** Add text field without any skip checks */
  JsonConverter addJsonTextFieldNoCheck(String key, String value) {
    mainNode.set(sanitizeValue(key), new TextNode(value));
    return this;
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  JsonConverter addJsonFieldNoCheck(ObjectNode node, String key, String value) {
    // Can be a json  or a string
    node.set(sanitizeValue(key), IS_COMPLEX_OBJECT.test(value) ? new POJONode(value) : new TextNode(value));
    return this;
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  JsonConverter addJsonRawFieldNoCheck(ObjectNode node, String key, String value) {
    // Can be a json  or a string
    node.set(sanitizeValue(key), new TextNode(value));
    return this;
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  JsonConverter addJsonFieldNoCheck(String key, String value) {
    return addJsonFieldNoCheck(mainNode, key, value);
  }

  SpecificRecordBase[] getBases() {
    return bases;
  }

  /** Applies all the replaceKeys to the value to remove all undesired patterns. */
  private String sanitizeValue(String value) {
    for (Pattern rule : replaceKeys) {
      value = rule.matcher(value).replaceAll("");
    }
    return value;
  }
}
