package org.gbif.pipelines.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

public class CleanDelimiterCharsUdf implements UDF1<String, String> {

  private static final CleanDelimiters CLEAN_DELIMITERS = new CleanDelimiters();

  @Override
  public String call(String field) throws Exception {
    return CLEAN_DELIMITERS.apply(field);
  }
}
