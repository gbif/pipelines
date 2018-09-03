package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Common converter, to convert any {@link SpecificRecordBase} object to json string
 *
 * <pre>{@code
 * Usage example:
 *
 * InterpretedExtendedRecord interRecord = ...
 * String result = SpecificRecordBase.create(interRecord).buildJson();
 *
 * }</pre>
 */
public class JsonConverter {

  final ObjectMapper mapper = new ObjectMapper();
  private final ObjectNode mainNode = mapper.createObjectNode();

  private final Map<Class<? extends SpecificRecordBase>, Consumer<SpecificRecordBase>>
      customConvertersMap = new HashMap<>();
  private SpecificRecordBase[] bases;
  private Set<String> skipKeys = Collections.emptySet();
  private String[] replaceKeys = {};

  JsonConverter() {}

  public static JsonConverter create() {
    return new JsonConverter();
  }

  public static JsonConverter create(SpecificRecordBase... bases) {
    return new JsonConverter().setSpecificRecordBase(bases);
  }

  public JsonConverter setSpecificRecordBase(SpecificRecordBase... bases) {
    this.bases = bases;
    return this;
  }

  public JsonConverter setReplaceKeys(String... replaceKeys) {
    this.replaceKeys = replaceKeys;
    return this;
  }

  /** Set keys, if you don't want to see them in json string */
  public JsonConverter setSkipKeys(String... escapeKeys) {
    if (this.skipKeys.isEmpty()) {
      this.skipKeys = new HashSet<>(Arrays.asList(escapeKeys));
    } else {
      this.skipKeys.addAll(Arrays.asList(escapeKeys));
    }
    return this;
  }

  /**
   * You want to use another way how to process a specific class, you can use your appender for this
   * object
   *
   * <pre>{@code
   * Example:
   *
   * BiConsumer<SpecificRecordBase, StringBuilder> funct = (record, sb) -> {
   *       Map<String, String> terms = ((ExtendedRecord) record).getCoreTerms();
   *       String example = map.get("Example");
   *       sb.append("\"exampleKey\":\"").append(example).append("\",");
   *     };
   * }</pre>
   */
  public JsonConverter addSpecificConverter(
      Class<? extends SpecificRecordBase> type, Consumer<SpecificRecordBase> consumer) {
    customConvertersMap.put(type, consumer);
    return this;
  }

  public ObjectNode buildJson() {
    for (SpecificRecordBase record : bases) {
      Consumer<SpecificRecordBase> consumer = customConvertersMap.get(record.getClass());
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
    base.getSchema()
        .getFields()
        .forEach(
            field ->
                Optional.ofNullable(base.get(field.pos()))
                    .map(Object::toString)
                    .ifPresent(r -> addJsonField(field.name(), r)));
    return this;
  }

  JsonConverter addJsonObject(String key, ObjectNode... nodes) {
    ObjectNode node = mapper.createObjectNode();
    Arrays.stream(nodes).forEach(node::setAll);
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonObject(String key, Map<String, String> fields) {
    ObjectNode node = mapper.createObjectNode();
    fields.forEach((k, v) -> addJsonField(node, k, v));
    mainNode.set(key, node);
    return this;
  }

  /** Check field in skipKeys and convert - "key":"value" */
  JsonConverter addJsonField(ObjectNode node, String key, String value) {
    return skipKeys.contains(key) ? this : addJsonFieldNoCheck(node, key, value);
  }

  /** Check field in skipKeys and convert - "key":"value" */
  JsonConverter addJsonField(String key, String value) {
    return addJsonField(mainNode, key, value);
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  JsonConverter addJsonFieldNoCheck(ObjectNode node, String key, String value) {
    for (String rule : replaceKeys) {
      key = key.replaceAll(rule, "");
    }
    // Can be a json as a string
    boolean isObject = value.startsWith("{\"") && value.endsWith("}");
    boolean isArrayOne = value.startsWith("[\"") && value.endsWith("]");
    boolean isArrayTwo = value.startsWith("[{") && value.endsWith("}]");
    if (isObject || isArrayOne || isArrayTwo) {
      node.set(key, new POJONode(value));
    } else {
      node.put(key, value);
    }
    return this;
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  JsonConverter addJsonFieldNoCheck(String key, String value) {
    return addJsonFieldNoCheck(mainNode, key, value);
  }

  SpecificRecordBase[] getBases() {
    return bases;
  }
}
