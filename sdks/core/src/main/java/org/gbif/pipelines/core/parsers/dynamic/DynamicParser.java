package org.gbif.pipelines.core.parsers.dynamic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Builder;

class DynamicParser {

  private final List<PatternDefault> patternList;
  private final Pattern unitsFromKey;
  private final Map<String, String> keyMap;
  private final String fieldName;

  private DynamicParser(
      List<PatternDefault> patternList,
      Pattern unitsFromKey,
      Map<String, String> keyMap,
      String fieldName) {
    this.patternList = patternList;
    this.unitsFromKey = unitsFromKey;
    this.keyMap = keyMap;
    this.fieldName = fieldName;
  }

  protected static DynamicParserBuilder builder() {
    return new DynamicParserBuilder();
  }

  protected Optional<DynamicProperty> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }

    try {
      for (PatternDefault p : patternList) {

        Matcher matcher = p.pattern.matcher(source.toLowerCase());

        if (matcher.find()) {
          String key = getGroup(matcher, p.defaultKey, "key");
          String value = getGroup(matcher, null, "value", "value1", "value2");
          String units = getGroup(matcher, p.defaulType, "units", "units1", "units2");
          if (key != null && units == null) {
            Matcher km = unitsFromKey.matcher(key);
            units = km.find() ? km.group() : null;
          }

          if (key != null && value != null && keyMap.containsKey(key)) {
            DynamicProperty property =
                DynamicProperty.builder()
                    .field(fieldName)
                    .key(keyMap.get(key))
                    .value(value)
                    .type(units)
                    .build();
            return Optional.of(property);
          }
        }
      }
    } catch (RuntimeException ex) {
      return Optional.empty();
    }

    return Optional.empty();
  }

  private String getGroup(Matcher matcher, String value, String... group) {
    String result = null;
    for (String g : group) {
      if (matcher.pattern().pattern().contains("<" + g + ">")) {
        String s = matcher.group(g);
        if (result == null) {
          result = s;
        } else {
          result = String.join(", ", result, s);
        }
      }
    }
    return result == null ? value : result;
  }

  static class DynamicParserBuilder {
    private final List<PatternDefault> patternList = new ArrayList<>();
    private final Map<String, String> mainTemplateMap = new LinkedHashMap<>();
    private final Map<String, String> keyMap = new HashMap<>();

    private Pattern unitsFromKey;
    private String fieldName;

    protected DynamicParserBuilder addUnitsFromKey(String unitsFromKey) {
      this.unitsFromKey = Pattern.compile(unitsFromKey);
      return this;
    }

    protected DynamicParserBuilder replaceRegexKeyGroupResultWith(
        String regexResult, String replace) {
      this.keyMap.put(regexResult, replace);
      return this;
    }

    protected DynamicParserBuilder parserName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    protected DynamicParserBuilder addRegexTemplate(String key, String value) {
      String result = value;
      for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
        result = result.replace(entry.getKey(), entry.getValue());
      }
      mainTemplateMap.put(key, result);
      return this;
    }

    protected DynamicParserBuilder addMatchPattern(String value) {
      return addMatchPattern(value, null, null);
    }

    protected DynamicParserBuilder addMatchPattern(
        String value, String defaultKey, String defaultType) {
      patternList.add(
          PatternDefault.builder()
              .stringPattern(value)
              .defaultKey(defaultKey)
              .defaulType(defaultType)
              .build());
      return this;
    }

    private void initParser() {

      for (Map.Entry<String, String> e : mainTemplateMap.entrySet()) {
        String v = e.getValue();
        for (Map.Entry<String, String> e2 : mainTemplateMap.entrySet()) {
          if (!e.getKey().equals(e2.getKey())) {
            v = v.replace(e2.getKey(), e2.getValue());
          }
        }
        mainTemplateMap.replace(e.getKey(), v);
      }

      for (PatternDefault p : patternList) {
        String result = p.stringPattern;
        for (Map.Entry<String, String> entry : mainTemplateMap.entrySet()) {
          result = result.replace(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
          result = result.replace(entry.getKey(), entry.getValue());
        }
        p.pattern = Pattern.compile(result);
        p.stringPattern = null;
      }
    }

    public DynamicParser build() {
      initParser();
      return new DynamicParser(patternList, unitsFromKey, keyMap, fieldName);
    }
  }

  @Builder
  private static final class PatternDefault {
    private Pattern pattern;
    private String stringPattern;
    private final String defaultKey;
    private final String defaulType;
  }
}
