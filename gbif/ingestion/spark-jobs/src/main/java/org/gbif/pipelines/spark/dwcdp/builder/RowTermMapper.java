package org.gbif.pipelines.spark.dwcdp.builder;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * Converts a Spark {@link Row} into a {@code Map<qualifiedTermUri, value>} suitable for populating
 * {@link org.gbif.pipelines.io.avro.ExtendedRecord} core/extension term maps.
 *
 * <p>Null values are omitted. Column names are resolved to qualified URIs via {@link TermResolver};
 * unresolvable names fall back to the raw column name.
 */
public final class RowTermMapper {

  private RowTermMapper() {}

  /**
   * Maps all columns in {@code columns} from {@code row} to qualified term URIs, skipping nulls.
   */
  public static Map<String, String> toTermMap(Row row, String[] columns) {
    Map<String, String> terms = new HashMap<>();
    for (String col : columns) {
      String value = safeGet(row, col);
      if (value != null) {
        terms.put(TermResolver.resolve(col), value);
      }
    }
    return terms;
  }

  /**
   * Returns the string value of {@code fieldName} from {@code row}, or {@code null} if absent or
   * null.
   */
  public static String safeGet(Row row, String fieldName) {
    try {
      int idx = row.fieldIndex(fieldName);
      if (row.isNullAt(idx)) {
        return null;
      }
      Object value = row.get(idx);
      return value == null ? null : value.toString();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
