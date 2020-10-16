package org.gbif.pipelines.core.converters;

import static org.apache.avro.Schema.Type.UNION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.primitives.Primitives;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Common converter, to convert any {@link SpecificRecordBase} object to json string
 *
 * <pre>{@code
 * Usage example:
 *
 * BasicRecord basic = ...
 * String result = JsonConverter.builder()
 *  .skipKey("decimalLatitude")
 *  .skipKey("decimalLongitude")
 *  .replaceKey(Pattern.compile("http://rs.tdwg.org/dwc/terms/"))
 *  .replaceKey(Pattern.compile("http://purl.org/dc/terms/"))
 *  .converter(ExtendedRecord.class, getExtendedRecordConverter())
 *  .converter(LocationRecord.class, getLocationRecordConverter())
 *  .converter(TemporalRecord.class, getTemporalRecordConverter())
 *  .converter(TaxonRecord.class, getTaxonomyRecordConverter())
 *  .converter(LocationFeatureRecord.class, getLocationFeatureRecordConverter())
 *  .build()
 *  .toString()
 *
 * }</pre>
 */
@SuppressWarnings("FallThrough")
@Slf4j
@Builder
public class JsonConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<Character, Character> CHAR_MAP = new HashMap<>();

  static {
    CHAR_MAP.put('\u001E', ',');
    CHAR_MAP.put('\u001f', ' ');
  }

  private final ObjectNode mainNode = MAPPER.createObjectNode();

  @Singular
  private Map<Class<? extends SpecificRecordBase>, BiConsumer<JsonConverter, SpecificRecordBase>>
      converters;

  @Singular private List<SpecificRecordBase> records;

  @Singular private Set<String> skipKeys;

  @Singular private List<Pattern> replaceKeys;

  public ObjectNode toJson() {
    for (SpecificRecordBase record : records) {
      BiConsumer<JsonConverter, SpecificRecordBase> consumer = converters.get(record.getClass());
      if (consumer != null) {
        consumer.accept(this, record);
      } else {
        addCommonFields(record);
      }
    }
    return mainNode;
  }

  @Override
  public String toString() {
    return toJson().toString();
  }

  private void addArrayNode(ObjectNode node, Schema.Field field, Collection<?> objects) {
    ArrayNode arrayNode = node.putArray(field.name());
    objects.forEach(
        value -> {
          if (value instanceof SpecificRecordBase) {
            ObjectNode element = createObjectNode();
            addCommonFields((SpecificRecordBase) value, element);
            arrayNode.add(element);
          } else if (value instanceof String
              || value.getClass().isPrimitive()
              || Primitives.isWrapperType(value.getClass())
              || UUID.class.isAssignableFrom(value.getClass())) {
            arrayNode.add(value.toString());
          }
        });
  }

  /**
   * Common way how to convert {@link SpecificRecordBase} to json string Converts {@link
   * SpecificRecordBase} by fields type and adds into {@link ObjectNode}
   */
  void addCommonFields(SpecificRecordBase record, ObjectNode node) {
    record.getSchema().getFields().stream()
        .filter(n -> !skipKeys.contains(n.name()))
        .forEach(
            f ->
                Optional.ofNullable(record.get(f.pos()))
                    .ifPresent(
                        r -> {
                          Schema schema = f.schema();
                          Optional<Type> type =
                              schema.getType() == UNION
                                  ? schema.getTypes().stream()
                                      .filter(t -> t.getType() != Type.NULL)
                                      .findFirst()
                                      .map(Schema::getType)
                                  : Optional.of(schema.getType());

                          type.ifPresent(
                              t -> {
                                switch (t) {
                                  case BOOLEAN:
                                    node.put(f.name(), (Boolean) r);
                                    break;
                                  case FLOAT:
                                  case DOUBLE:
                                    node.put(f.name(), (Double) r);
                                    break;
                                  case INT:
                                    node.put(f.name(), (Integer) r);
                                    break;
                                  case LONG:
                                    node.put(f.name(), (Long) r);
                                    break;
                                  case RECORD:
                                    if (r instanceof SpecificRecordBase) {
                                      ObjectNode recordNode = createObjectNode();
                                      addCommonFields((SpecificRecordBase) r, recordNode);
                                      node.set(f.name(), recordNode);
                                    }
                                    break;
                                  case ARRAY:
                                    Collection values = (Collection) r;
                                    if (!values.isEmpty()) {
                                      addArrayNode(node, f, (Collection) r);
                                    }
                                    break;
                                  default:
                                    addJsonRawFieldNoCheck(node, f.name(), r.toString());
                                    break;
                                }
                              });
                        }));
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  void addCommonFields(SpecificRecordBase base) {
    addCommonFields(base, mainNode);
  }

  void addJsonObject(String key, ObjectNode... nodes) {
    ObjectNode node = MAPPER.createObjectNode();
    Arrays.stream(nodes).forEach(node::setAll);
    mainNode.set(key, node);
  }

  void addJsonArray(String key, Collection<? extends JsonNode> values) {
    ArrayNode node = mainNode.putArray(key);
    node.addAll(values);
  }

  /** Checks field in skipKeys and convert - "key":"value" */
  void addJsonRawField(ObjectNode node, String key, String value) {
    if (!skipKeys.contains(key)) {
      addJsonRawFieldNoCheck(node, key, value);
    }
  }

  /** Adds text field without any skip checks */
  void addJsonTextFieldNoCheck(String key, String value) {
    mainNode.set(sanitizeValue(key), getEscapedTextNode(value));
  }

  /** Converts - "key":"value" and check some incorrect symbols for json */
  void addJsonRawFieldNoCheck(ObjectNode node, String key, String value) {
    // Can be a json  or a string
    node.set(sanitizeValue(key), getEscapedTextNode(value));
  }

  ObjectNode getMainNode() {
    return mainNode;
  }

  static TextNode getEscapedTextNode(String value) {
    for (Entry<Character, Character> rule : CHAR_MAP.entrySet()) {
      value = value.replace(rule.getKey(), rule.getValue());
    }
    return new TextNode(value);
  }

  /** Creates a empty ArrayNode. */
  static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /** Creates an empty ObjectNode. */
  static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /** Applies all the replaceKeys to the value to remove all undesired patterns. */
  private String sanitizeValue(String value) {
    for (Pattern rule : replaceKeys) {
      value = rule.matcher(value).replaceAll("");
    }
    return value;
  }
}
