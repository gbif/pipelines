package org.gbif.pipelines.core.parsers.dynamic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.NoArgsConstructor;

@NoArgsConstructor(staticName = "create")
class DynamicParser {

  private final Map<String, String> mainTemplateMap = new LinkedHashMap<>();

  protected void put(String key, String value) {
    String result = value;
    for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    mainTemplateMap.put(key, result);
  }

  protected Pattern initPattern(String value) {
    String result = value;
    for (Map.Entry<String, String> entry : mainTemplateMap.entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : RegexDefaultTemplates.get().entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    return Pattern.compile(result);
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

  protected void initMap() {
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
}
