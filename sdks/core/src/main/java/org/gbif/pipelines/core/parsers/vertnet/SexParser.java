package org.gbif.pipelines.core.parsers.vertnet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/trait_parsers/sex_parser.py
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SexParser {

  private static final Pattern SEX_KEY_VALUE_DELIMITED =
      Pattern.compile("\\b(?<key>sex)\\W+(?<value>[\\w?.]+(?:\\s+[\\w?.]+){0,2})\\s*(?:[:;,\"]|$)");
  private static final Pattern SEX_KEY_VALUE_UNDELIMITED =
      Pattern.compile("\\b(?<key>sex)\\W+(?<value>\\w+)");
  private static final Pattern SEX_UNKEYED =
      Pattern.compile("\\b(?<value>(?:males?|females?)(?:\\s*\\?)?)\\b");

  private static final List<Pattern> PATTERNS =
      Arrays.asList(SEX_KEY_VALUE_DELIMITED, SEX_KEY_VALUE_UNDELIMITED, SEX_UNKEYED);

  public static Optional<String> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }

    try {
      for (Pattern p : PATTERNS) {
        Matcher matcher = p.matcher(source.toLowerCase());
        String result = matcher.find() ? matcher.group("value") : null;
        if (result != null) {
          while (matcher.find()) {
            String group = matcher.group("value");
            if (!group.equals(result)) {
              return Optional.empty();
            }
          }
          return Optional.of(result);
        }
      }
    } catch (RuntimeException ex) {
      return Optional.empty();
    }

    return Optional.empty();
  }
}
