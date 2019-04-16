package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import static org.apache.avro.Schema.Type.UNION;

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
 *  .converter(AustraliaSpatialRecord.class, getAustraliaSpatialRecordConverter())
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

  // Utility predicates to check if a node is a complex element
  private static final Predicate<String> IS_OBJECT = value -> value.startsWith("{\"") && value.endsWith("}");
  private static final Predicate<String> IS_ARRAY_ONE = value -> value.startsWith("[") && value.endsWith("]");
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

  @Singular
  private Map<Class<? extends SpecificRecordBase>, BiConsumer<JsonConverter, SpecificRecordBase>> converters;

  @Singular
  private List<SpecificRecordBase> records;

  @Singular
  private Set<String> skipKeys;

  @Singular
  private List<Pattern> replaceKeys;

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

  private void addArrayNode(ObjectNode node, Schema.Field field,Collection<?> objects) {
    ArrayNode arrayNode = node.putArray(field.name());
    objects.forEach( value -> {
      if(value instanceof SpecificRecordBase) {
        ObjectNode element = createObjectNode();
        addCommonFields((SpecificRecordBase)value, element);
        arrayNode.add(element);
      }
    });
  }
  /**
   * Common way how to convert {@link SpecificRecordBase} to json string
   * Converts {@link SpecificRecordBase} by fields type and adds into {@link ObjectNode}
   */
  void addCommonFields(SpecificRecordBase record, ObjectNode node) {
    record.getSchema().getFields().stream()
        .filter(n -> !skipKeys.contains(n.name()))
        .forEach(
            f -> Optional.ofNullable(record.get(f.pos())).ifPresent(r -> {

              Schema schema = f.schema();
              Optional<Type> type = schema.getType() == UNION
                  ? schema.getTypes().stream().filter(t -> t.getType() != Type.NULL).findFirst().map(Schema::getType)
                  : Optional.of(schema.getType());

              type.ifPresent(t -> {
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
                    Collection values = (Collection)r;
                    if(!values.isEmpty()) {
                      addArrayNode(node, f, (Collection)r);
                    }
                    break;
                  default:
                    addJsonFieldNoCheck(node, f.name(), r.toString());
                    break;
                }
              });
            })
        );
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  void addCommonFields(SpecificRecordBase base) {
    addCommonFields(base, mainNode);
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  void addCommonFields(String key, SpecificRecordBase base) {
    ObjectNode node = MAPPER.createObjectNode();
    addCommonFields(base, node);
    mainNode.set(key, node);
  }

  void addJsonObject(String key, ObjectNode... nodes) {
    ObjectNode node = MAPPER.createObjectNode();
    Arrays.stream(nodes).forEach(node::setAll);
    mainNode.set(key, node);
  }

  void addSingleJsonObject(String key, ObjectNode node) {
    mainNode.set(key, node);
  }

  void addJsonComplexObject(String key, Map<String, String> fields) {
    ObjectNode node = MAPPER.createObjectNode();
    fields.forEach((k, v) -> addJsonField(node, k, v));
    mainNode.set(key, node);
  }

  void addJsonRawObject(String key, Map<String, String> fields) {
    ObjectNode node = MAPPER.createObjectNode();
    fields.forEach((k, v) -> addJsonRawField(node, k, v));
    mainNode.set(key, node);
  }

  void addJsonArray(String key, Collection<? extends JsonNode> values) {
    ArrayNode node = mainNode.putArray(key);
    node.addAll(values);
  }

  /** Checks field in skipKeys and convert - "key":"value" */
  void addJsonField(ObjectNode node, String key, String value) {
    if (!skipKeys.contains(key)) {
      addJsonFieldNoCheck(node, key, value);
    }
  }

  /** Checks field in skipKeys and convert - "key":"value" */
  void addJsonRawField(ObjectNode node, String key, String value) {
    if (!skipKeys.contains(key)) {
      addJsonRawFieldNoCheck(node, key, value);
    }
  }

  /** Checks field in skipKeys and convert - "key":"value" */
  void addJsonField(String key, String value) {
    addJsonField(mainNode, key, value);
  }

  /** Adds text field without any skip checks */
  void addJsonTextFieldNoCheck(String key, String value) {
    mainNode.set(sanitizeValue(key), new TextNode(value));
  }

  /** Converts - "key":"value" and check some incorrect symbols for json */
  void addJsonFieldNoCheck(ObjectNode node, String key, String value) {
    // Can be a json  or a string
    node.set(sanitizeValue(key), IS_COMPLEX_OBJECT.test(value) ? new POJONode(value) : new TextNode(value));
  }

  /** Converts - "key":"value" and check some incorrect symbols for json */
  void addJsonRawFieldNoCheck(ObjectNode node, String key, String value) {
    // Can be a json  or a string
    node.set(sanitizeValue(key), new TextNode(value));
  }

  /** Converts - "key":"value" and check some incorrect symbols for json */
  void addJsonFieldNoCheck(String key, String value) {
    addJsonFieldNoCheck(mainNode, key, value);
  }

  public ObjectNode getMainNode() {
    return mainNode;
  }

  /**
   * Creates a empty ArrayNode.
   */
  static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /**
   * Creates an empty ObjecNode.
   */
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

  public static ObjectMapper mapper() {
    return MAPPER;
  }
}
