package org.gbif.pipelines.indexing.converter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

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
  private Map<Class<? extends SpecificRecordBase>, BiConsumer<SpecificRecordBase, StringBuilder>>
      biConsumer = new HashMap<>();
  private String[] replaceKeys = {};

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
      Class<? extends SpecificRecordBase> type,
      BiConsumer<SpecificRecordBase, StringBuilder> consumer) {
    biConsumer.put(type, consumer);
    return this;
  }

  public String buildJson() {
    Arrays.stream(bases)
        .forEach(
            record -> {
              BiConsumer<SpecificRecordBase, StringBuilder> consumer =
                  biConsumer.get(record.getClass());
              if (Objects.nonNull(consumer)) {
                consumer.accept(record, sb);
              } else {
                commonConvert(record);
              }
            });
    return filterAndConvert();
  }

  /** Common way how to convert {@link SpecificRecordBase} to json string */
  Records2JsonConverter commonConvert(SpecificRecordBase base) {
    base.getSchema()
        .getFields()
        .forEach(field -> addJsonField(field.name(), base.get(field.pos())));
    return this;
  }

  /** Filter possible incorrect symbols for json - \ or }{ and etc. */
  private String filterAndConvert() {
    append("}");
    return sb.toString()
        .replaceAll("(\"\\{)|(\\{,)", "{")
        .replaceAll("(}\")|(,})", "}")
        .replaceAll("(\\[,)", "[")
        .replaceAll("(\"\\[\\{)", "[{")
        .replaceAll("(}]\")", "}]")
        .replaceAll("(,])", "]")
        .replaceAll("(}]\\[\\{)", "],[")
        .replaceAll("(\"\")", "\",\"")
        .replaceAll("(}\\{)", "},{");
  }

  Records2JsonConverter append(Object obj) {
    sb.append(obj);
    return this;
  }

  /** Check field in escapeKeys and convert - "key":"value" */
  Records2JsonConverter addJsonField(String key, Object value) {
    if (escapeKeys.contains(key)) {
      return this;
    }
    return addJsonFieldNoCheck(key, value);
  }

  /** Convert - "key":"value" and check some incorrect symbols for json*/
  Records2JsonConverter addJsonFieldNoCheck(String key, Object value) {
    for (String rule : replaceKeys) {
      key = key.replaceAll(rule, "");
    }
    sb.append("\"").append(key).append("\":");
    if (Objects.isNull(value)) {
      return append("null").append(",");
    }
    if (value instanceof String) {
      value = ((String) value).replaceAll("(\\\\)", "\\\\\\\\").replaceAll("\"", "\\\\\"");
    }
    return append("\"").append(value).append("\",");
  }
}
