package org.gbif.pipelines.spark.udf;

import java.io.Serializable;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

public class CleanDelimiters implements Function<String, String>, Serializable {

  private static String cleanDelimiters(String value) {
    return DELIMITERS_MATCH_PATTERN.matcher(value).replaceAll(" ").trim();
  }

  public static final String DELIMITERS_MATCH =
      "\\t|\\n|\\r|(?:(?>\\u000D\\u000A)|[\\u000A\\u000B\\u000C\\u000D\\u0085\\u2028\\u2029\\u0000])";

  public static final Pattern DELIMITERS_MATCH_PATTERN = Pattern.compile(DELIMITERS_MATCH);

  @Override
  @SneakyThrows
  public String apply(String value) {
    return value != null ? cleanDelimiters(value) : null;
  }
}
