package org.gbif.pipelines.core.parsers.dynamic;

import static org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.gbif.pipelines.io.avro.DynamicProperty;

@AllArgsConstructor(staticName = "create")
class DynamicParser {

  private final Map<String, String> mainTemplateMap = new LinkedHashMap<>();
  private final List<PatternDefault> patterns = new ArrayList<>();
  private final Pattern unitsFromKey;
  private final Map<String, String> keyMap;

  protected void addTemplate(String key, String value) {
    String result = value;
    for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    mainTemplateMap.put(key, result);
  }

  protected void initPattern(String value) {
    initPattern(value, null, null);
  }

  protected void initPattern(String value, String defaultKey, String defaultType) {
    String result = value;
    for (Map.Entry<String, String> entry : mainTemplateMap.entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    patterns.add(
        PatternDefault.builder()
            .pattern(Pattern.compile(result))
            .defaultKey(defaultKey)
            .defaulType(defaultType)
            .build());
    System.out.println(result);
  }

  protected String getGroup(Matcher matcher, String value, String... group) {
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

  protected void initTemplates() {
    for (Map.Entry<String, String> e : mainTemplateMap.entrySet()) {
      String v = e.getValue();
      for (Map.Entry<String, String> e2 : mainTemplateMap.entrySet()) {
        if (!e.getKey().equals(e2.getKey())) {
          v = v.replace(e2.getKey(), e2.getValue());
        }
      }
      mainTemplateMap.replace(e.getKey(), v);
    }
  }

  protected Optional<DynamicProperty> parse(String source) {
    if (source == null || source.isEmpty()) {
      return Optional.empty();
    }

    try {
      for (PatternDefault p : patterns) {

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
            DynamicProperty dynamicProperty =
                DynamicProperty.newBuilder()
                    .setKey(keyMap.get(key))
                    .setValue(value)
                    .setClazz(Type.STRING)
                    .setType(units)
                    .build();
            return Optional.of(dynamicProperty);
          }
        }
      }
    } catch (RuntimeException ex) {
      return Optional.empty();
    }

    return Optional.empty();
  }

  @Builder
  protected static final class PatternDefault {
    private final Pattern pattern;
    private final String defaultKey;
    private final String defaulType;
  }
}
