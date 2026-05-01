package org.gbif.pipelines.spark.udf;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/**
 * A simple UDF for Hive that replaces specials characters with blanks. The characters replaced by
 * this UDF can break a download format and those are: tabs, line breaks and new lines. If the input
 * value is null or can't be parsed, an empty string is returned.
 */
public class CleanDelimiterArraysUdf implements UDF1<WrappedArray<String>, String[]> {

  private static final CleanDelimiters CLEAN_DELIMITERS = new CleanDelimiters();

  @Override
  public String[] call(WrappedArray<String> field) throws Exception {
    return field != null && !field.isEmpty() ? toArray(field) : null;
  }

  /** Converts to an array, returns null if the produced array is empty. */
  private String[] toArray(WrappedArray<String> field) {
    String[] value =
        JavaConverters.asJavaCollection(field).stream()
            .map(CLEAN_DELIMITERS)
            .filter(s -> s != null && !s.isEmpty())
            .toArray(String[]::new);
    return value.length > 0 ? value : null;
  }
}
