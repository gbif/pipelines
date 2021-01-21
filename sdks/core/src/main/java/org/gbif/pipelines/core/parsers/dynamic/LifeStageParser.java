package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LifeStageParser {

  private static final Pattern EXCLUDE_PATTERN = Pattern.compile("determin");
  private static final Pattern LIFE_STAGE_KEY_VALUE_DELIMITED =
      Pattern.compile(
          "\\b(?<key>(?:life\\s*stage(?:\\s*remarks)?|age(?:\\s*class)?))\\W+(?<value>[\\w?./\\-]+(?:\\s+[\\w?./\\-]+){0,4})\\s*(?:[:;,\"]|$)");
  private static final Pattern LIFE_STAGE_KEY_VALUE_UNDELIMITED =
      Pattern.compile(
          "\\b(?<key>life\\s*stage(?:\\s*remarks)?|age\\s*class|age\\s*in\\s*(?:hour|day)s?|age)\\W+(?<value>[\\w?./\\-]+(?:\\s+(?:year|recorded))?)");
  private static final Pattern LIFE_STAGE_NO_KEYWORD =
      Pattern.compile("(?<value>(?:after\\s+)?(?:first|second|third|fourth|hatching)\\s+year)");
  private static final Pattern LIFE_STAGE_YOLK_SAC = Pattern.compile("(?<value>(?:yolk)\\s+sac)");
  private static final Pattern LIFE_STAGE_UNKEYED =
      Pattern.compile(
          "\\b(?<value>(?:larves?|larvae?|larvals?|imagos?|neonates?|hatchlings?|hatched?|fry?|metamorphs?|premetamorphs"
              + "|tadpoles?|têtard?|young-of-the-year?|leptocephales?|leptocephalus?|immatures?|imms?|jeunes?"
              + "|young?|ygs?|fleglings?|fledgelings?|chicks?|nestlings?|juveniles?|juvéniles?|juvs?|subadults?"
              + "|subadultes?|subads?|sub-adults?|yearlings?|matures?|adults?|adulte?|ads?)(?:\\s*\\?)?)\\b");

  private static final List<Pattern> PATTERNS =
      Arrays.asList(
          LIFE_STAGE_KEY_VALUE_DELIMITED,
          LIFE_STAGE_KEY_VALUE_UNDELIMITED,
          LIFE_STAGE_NO_KEYWORD,
          LIFE_STAGE_YOLK_SAC,
          LIFE_STAGE_UNKEYED);

  public static Optional<String> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }
    Matcher exclude = EXCLUDE_PATTERN.matcher(source.toLowerCase());
    if (exclude.find()) {
      return Optional.empty();
    }
    for (Pattern p : PATTERNS) {
      Matcher matcher = p.matcher(source.toLowerCase());
      if (matcher.find()) {
        return Optional.ofNullable(matcher.group("value"));
      }
    }
    return Optional.empty();
  }
}
