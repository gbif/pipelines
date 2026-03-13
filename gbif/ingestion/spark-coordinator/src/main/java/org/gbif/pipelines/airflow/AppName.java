package org.gbif.pipelines.airflow;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.pipelines.common.PipelinesException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AppName {

  private static final Set<String> IGNORE_SET = Set.of("to");

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max length is 64 characters.
   */
  public static String get(StepType type, UUID datasetKey, int attempt) {
    String joined =
        String.join("-", shortenType(type), datasetKey.toString(), String.valueOf(attempt));
    if (joined.length() >= 64) {
      throw new PipelinesException("Spark name can't be normalized, cause run_id length > 64 char");
    }
    return joined;
  }

  private static String shortenType(StepType type) {
    return Arrays.stream(type.name().split("_"))
        .map(
            s -> {
              int l = s.length() > 4 ? 5 : s.length();
              return s.toLowerCase().substring(0, l);
            })
        .filter(s -> !IGNORE_SET.contains(s))
        .collect(Collectors.joining("-"));
  }
}
