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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.specific.SpecificRecordBase;

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
public class JsonConverter {

  static final ObjectMapper MAPPER = new ObjectMapper();

  //Utility predicates to check if a node is a complex element
  private static final Predicate<String> IS_OBJECT = nodeValue -> nodeValue.startsWith("{\"") && nodeValue.endsWith("}");
  private static final Predicate<String> IS_ARRAY_ONE = nodeValue -> nodeValue.startsWith("[\"") && nodeValue.endsWith("]");
  private static final Predicate<String> IS_ARRAY_TWO = nodeValue -> nodeValue.startsWith("[{") && nodeValue.endsWith("}]");

  private static final Predicate<String> IS_COMPLEX_OBJECT = IS_OBJECT.or(IS_ARRAY_ONE.or(IS_ARRAY_TWO));

  private final ObjectNode mainNode = MAPPER.createObjectNode();

  private final Map<Class<? extends SpecificRecordBase>, Consumer<SpecificRecordBase>>
      customConvertersMap = new HashMap<>();
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
   * Applies all the replaceKeys to the value to remove all undesired patterns.
   */
  private String sanitizeValue(String value) {
    for (Pattern rule : replaceKeys) {
      value = rule.matcher(value).replaceAll("");
    }
    return value;
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
  JsonConverter addSpecificConverter(Class<? extends SpecificRecordBase> type,
                                     Consumer<SpecificRecordBase> consumer) {
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
    ObjectNode node = MAPPER.createObjectNode();
    Arrays.stream(nodes).forEach(node::setAll);
    mainNode.set(key, node);
    return this;
  }

  JsonConverter addJsonObject(String key, Map<String, String> fields) {
    ObjectNode node = MAPPER.createObjectNode();
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
    // Can be a json  or a string
    node.set(sanitizeValue(key), IS_COMPLEX_OBJECT.test(value)? new POJONode(value) : new TextNode(value));
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
