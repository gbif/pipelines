package org.gbif.pipelines.core.converters;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.terms.utils.TermUtils;

@Slf4j
public class ConverterUtils {

  public static final String CLEANING_MATCH_REGEX =
      "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern CLEANING_MATCH_PATTERN = Pattern.compile(CLEANING_MATCH_REGEX);

  private static String cleanVerbatim(String value) {
    if (value == null) return null;
    return CLEANING_MATCH_PATTERN.matcher(value).replaceAll(" ").trim();
  }

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  public static void mapTerm(String k, String value, Object hdfsRecord) {
    Term term = TERM_FACTORY.findTerm(k);

    if (term == null || value == null) {
      return;
    }

    if (TermUtils.verbatimTerms().contains(term)) {
      Optional.ofNullable(verbatimSchemaField(term))
          .ifPresent(
              field -> {
                try {
                  String verbatimField =
                      "V" + StringUtils.capitalize(term.simpleName().toLowerCase());

                  // special case for class, as always
                  if (DwcTerm.class_ == term) {
                    verbatimField = "VClass_";
                  }
                  String normalized =
                      StringUtils.trimToEmpty(
                          cleanVerbatim(value).replaceAll("[\\p{Z}\\p{Cc}\\p{Cf}]+", " "));
                  PropertyUtils.setProperty(hdfsRecord, verbatimField, normalized);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }

    if (!TermUtils.isInterpretedSourceTerm(term)) {
      Optional.ofNullable(interpretedSchemaField(term, hdfsRecord.getClass()))
          .ifPresent(
              field -> {
                // Fields that were set by other mappers are ignored

                // Use reflection to get the value (previously used names from Avro schema)
                String methodName =
                    "get"
                        + Character.toUpperCase(field.getName().charAt(0))
                        + field.getName().substring(1);
                try {
                  Method method = hdfsRecord.getClass().getMethod(methodName);
                  if (Objects.isNull(method.invoke(hdfsRecord))) {
                    String interpretedFieldname = field.getName();
                    if (DcTerm.abstract_ == term) {
                      interpretedFieldname = "abstract$";
                    } else if (DwcTerm.class_ == term) {
                      interpretedFieldname = "class$";
                    } else if (DwcTerm.group == term) {
                      interpretedFieldname = "group";
                    } else if (DwcTerm.order == term) {
                      interpretedFieldname = "order";
                    } else if (DcTerm.date == term) {
                      interpretedFieldname = "date";
                    } else if (DcTerm.format == term) {
                      interpretedFieldname = "format";
                    }
                    setHdfsRecordField(hdfsRecord, field, interpretedFieldname, v);
                  }
                } catch (IllegalAccessException
                    | InvocationTargetException
                    | NoSuchMethodException ex) {
                  throw new RuntimeException(ex);
                }
              });
    }
  }

  /** Gets the {@link Schema.Field} associated to a verbatim term. */
  private static Field verbatimSchemaField(Term term) {
    try {

      if (term == DwcTerm.class_) {
        return OccurrenceHdfsRecord.class.getDeclaredField("vClass_");
      }

      return OccurrenceHdfsRecord.class.getDeclaredField(
          "v" + StringUtils.capitalize(term.simpleName().toLowerCase()));
    } catch (NoSuchFieldException e) {
      return null; // Field not found
    }
  }

  /** Gets the {@link Schema.Field} associated to a interpreted term. */
  private static Field interpretedSchemaField(Term term, Class theClass) {
    try {
      return theClass.getDeclaredField(HiveColumns.columnFor(term));
    } catch (NoSuchFieldException e) {
      return null; // Field not found
    }
  }

  /**
   * From a {@link Schema.Field} copies it value into a the {@link OccurrenceHdfsRecord} field using
   * the recognized data type.
   *
   * @param hdfsRecord target record
   * @param fieldName {@link OccurrenceHdfsRecord} field/property name
   * @param value field data/value
   */
  private static void setHdfsRecordField(
      Object hdfsRecord, Field field, String fieldName, String value) {
    try {
      Type fieldType = field.getGenericType();
      if (fieldType.equals(Integer.class)) {
        PropertyUtils.setProperty(hdfsRecord, fieldName, Integer.valueOf(value));
      } else if (fieldType.equals(Long.class)) {
        PropertyUtils.setProperty(hdfsRecord, fieldName, Long.valueOf(value));
      } else if (fieldType.equals(Boolean.class)) {
        PropertyUtils.setProperty(hdfsRecord, fieldName, Boolean.valueOf(value));
      } else if (fieldType.equals(Double.class)) {
        PropertyUtils.setProperty(hdfsRecord, fieldName, Double.valueOf(value));
      } else if (fieldType.equals(Float.class)) {
        PropertyUtils.setProperty(hdfsRecord, fieldName, Float.valueOf(value));
      } else {
        PropertyUtils.setProperty(hdfsRecord, fieldName, value);
      }
    } catch (Exception ex) {
      log.debug(
          "Ignoring error setting field {}, field name {}, value. Exception: {}",
          field,
          fieldName,
          value);
    }
  }

  public static String base64Encode(String original) {
    if (original == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(original.getBytes(StandardCharsets.UTF_8));
  }
}
