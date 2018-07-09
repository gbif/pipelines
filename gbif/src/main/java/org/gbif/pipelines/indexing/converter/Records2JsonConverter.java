package org.gbif.pipelines.indexing.converter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
public class Records2JsonConverter {

  private final StringBuilder sb = new StringBuilder().append("{");
  private SpecificRecordBase[] bases;
  private Set<String> escapeKeys = Collections.emptySet();
  private Map<Class<? extends SpecificRecordBase>, Consumer<SpecificRecordBase>>
      customConvertersMap = new HashMap<>();
  private String[] replaceKeys = {};

  private static final Pattern PATTERN1 = Pattern.compile("(\\\\)", Pattern.DOTALL);
  private static final Pattern PATTERN2 = Pattern.compile("\"", Pattern.DOTALL);
  private static final Pattern PATTERN3 = Pattern.compile("(\"\\{)|(\\{,)", Pattern.DOTALL);
  private static final Pattern PATTERN4 = Pattern.compile("(}\")|(,})", Pattern.DOTALL);
  private static final Pattern PATTERN5 = Pattern.compile("(\\[,)", Pattern.DOTALL);
  private static final Pattern PATTERN6 = Pattern.compile("(\"\\[\\{)", Pattern.DOTALL);
  private static final Pattern PATTERN7 = Pattern.compile("(}]\")", Pattern.DOTALL);
  private static final Pattern PATTERN8 = Pattern.compile("(,])", Pattern.DOTALL);
  private static final Pattern PATTERN9 = Pattern.compile("(}]\\[\\{)", Pattern.DOTALL);
  private static final Pattern PATTERN10 = Pattern.compile("(\"\")", Pattern.DOTALL);
  private static final Pattern PATTERN11 = Pattern.compile("(}\\{)", Pattern.DOTALL);

  Records2JsonConverter() {}

  public static Records2JsonConverter create() {
    return new Records2JsonConverter();
  }

  public static Records2JsonConverter create(SpecificRecordBase... bases) {
    return new Records2JsonConverter().setSpecificRecordBase(bases);
  }

  public Records2JsonConverter setSpecificRecordBase(SpecificRecordBase... bases) {
    this.bases = bases;
    return this;
  }

  public Records2JsonConverter setReplaceKeys(String... replaceKeys) {
    this.replaceKeys = replaceKeys;
    return this;
  }

  /** Set keys, if you don't want to see them in json string */
  public Records2JsonConverter setEscapeKeys(String... escapeKeys) {
    if (this.escapeKeys.isEmpty()) {
      this.escapeKeys = new HashSet<>(Arrays.asList(escapeKeys));
    } else {
      this.escapeKeys.addAll(Arrays.asList(escapeKeys));
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
  public Records2JsonConverter addSpecificConverter(
      Class<? extends SpecificRecordBase> type, Consumer<SpecificRecordBase> consumer) {
    this.customConvertersMap.put(type, consumer);
    return this;
  }

  public String buildJson() {
    Arrays.stream(bases)
        .forEach(
            record -> {
              Consumer<SpecificRecordBase> consumer =
                  this.customConvertersMap.get(record.getClass());
              if (Objects.nonNull(consumer)) {
                consumer.accept(record);
              } else {
                addCommonFields(record);
              }
            });
    return filterAndConvert();
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  Records2JsonConverter addCommonFields(SpecificRecordBase base) {
    base.getSchema()
        .getFields()
        .forEach(field -> addJsonField(field.name(), base.get(field.pos())));
    return this;
  }

  /** Filter possible incorrect symbols for json - \ or }{ and etc. */
  private String filterAndConvert() {
    append("}");
    String r3 = PATTERN3.matcher(sb.toString()).replaceAll("{");
    String r4 = PATTERN4.matcher(r3).replaceAll("}");
    String r5 = PATTERN5.matcher(r4).replaceAll("[");
    String r6 = PATTERN6.matcher(r5).replaceAll("[{");
    String r7 = PATTERN7.matcher(r6).replaceAll("}]");
    String r8 = PATTERN8.matcher(r7).replaceAll("]");
    String r9 = PATTERN9.matcher(r8).replaceAll("}],[{");
    String r10 = PATTERN10.matcher(r9).replaceAll("\",\"");
    return PATTERN11.matcher(r10).replaceAll("},{");
  }

  private Records2JsonConverter append(Object obj) {
    sb.append(obj);
    return this;
  }

  Records2JsonConverter addJsonObject(String key, JsonFiled... fields) {
    Map<String, String> map =
        Arrays.stream(fields).collect(Collectors.toMap(JsonFiled::getKey, JsonFiled::getValue));
    return addJsonObject(key, map);
  }

  Records2JsonConverter addJsonObject(String key, Map<String, String> fields) {
    append("\"").append(key).append("\":");
    if (!fields.isEmpty()) {
      append("{");
      fields.forEach(this::addJsonField);
      append("},");
    } else {
      append("null,");
    }
    return this;
  }

  /** Check field in escapeKeys and convert - "key":"value" */
  Records2JsonConverter addJsonField(String key, Object value) {
    if (escapeKeys.contains(key)) {
      return this;
    }
    return addJsonFieldNoCheck(key, value);
  }

  /** Convert - "key":"value" and check some incorrect symbols for json */
  Records2JsonConverter addJsonFieldNoCheck(String key, Object value) {
    for (String rule : replaceKeys) {
      key = key.replaceAll(rule, "");
    }
    sb.append("\"").append(key).append("\":");
    if (Objects.isNull(value)) {
      return append("null,");
    }
    if (value instanceof String) {
      String r1 = PATTERN1.matcher(((String) value)).replaceAll("\\\\\\\\");
      value = PATTERN2.matcher(r1).replaceAll("\\\\\"");
    }
    return append("\"").append(value).append("\",");
  }

  public static class JsonFiled {

    private final String key;
    private final String value;

    private JsonFiled(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public static JsonFiled create(String key, String value) {
      return new JsonFiled(key, value);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }
}
