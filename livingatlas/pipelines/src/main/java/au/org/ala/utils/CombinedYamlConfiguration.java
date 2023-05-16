package au.org.ala.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.SneakyThrows;
import org.gbif.pipelines.common.PipelinesException;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

public class CombinedYamlConfiguration {

  private final Map<String, String> mainArgs = new LinkedHashMap<>();
  private final Map<String, Object> combined;

  public CombinedYamlConfiguration(String... mainArgs) throws IOException {
    // First: copy main args to map
    for (String arg : mainArgs) {
      // For each arg of type --varName=value we remove the -- and split by = in varName and value
      String[] argPair = arg.replaceFirst("--", "").split("=", 2);
      // And we combine the result
      if (argPair.length != 2) {
        throw new PipelinesException("Badly formatted argument:  " + arg);
      }
      this.mainArgs.put(argPair[0], argPair[1]);
    }
    // Look for a config arg to find the yaml file paths that can be comma separated
    String config = this.mainArgs.get("config");
    if (config == null) {
      throw new PipelinesException(
          "The --config=\"some-config.yml,some-other.yml\" argument is missing");
    }
    String[] yamlConfigPaths = config.split(",");
    // we remove config, because is not an pipeline configuration
    this.mainArgs.remove("config");

    // Convert the main args to a two-dimensional Array
    String[][] mainArgsAsList =
        new String[][] {
          this.mainArgs.keySet().toArray(new String[0]),
          this.mainArgs.values().toArray(new String[0])
        };

    // Load each yaml, and combine the values
    Map<String, Object> merged = new LinkedHashMap<>();
    for (String path : yamlConfigPaths) {
      Map<String, Object> loaded;
      try (InputStream input = new FileInputStream(path)) {
        Yaml yaml = new Yaml();
        loaded = yaml.load(input);
      }
      if (loaded != null) {
        // This means that config is not empty
        merged = combineMap(merged, loaded);
      }
    }

    // Substitute vars like {datasetId} in the yaml
    // for this we dump the merged configs to a String, we replace that args, and we reload the yaml
    Yaml mergedYaml = new Yaml();
    Yaml mergedWithVars = new Yaml();
    combined = mergedWithVars.load(substituteVars(mergedYaml.dump(merged), mainArgsAsList));

    // Remove pipelineExcludeArgs as they are not used by PipelineOptions
    Object excludeArgs = get("pipelineExcludeArgs");
    if (excludeArgs != null) {
      for (String excludeArg : excludeArgs.toString().split(",")) {
        this.mainArgs.remove(excludeArg.trim());
      }
    }
  }

  /**
   * Combine linked hash maps.
   *
   * <p>If some keys are maps of values we join them. For instance:
   *
   * <pre>
   *      # map1
   *      general:
   *        a: valueA
   *        b: valueB
   *
   *      # map2
   *      general:
   *        a: valueA'
   *        c: valueC
   *
   *      # combinedMap:
   *      general:
   *        a: valueA'
   *        b: valueB
   *        c: valueC
   * </pre>
   *
   * @param map1 the map 1
   * @param map2 the map 2
   * @return the merged hash map
   */
  private Map<String, Object> combineMap(Map<String, Object> map1, Map<String, Object> map2) {
    Map<String, Object> map3 = new LinkedHashMap<>(map1);
    for (Map.Entry<String, Object> entry : map2.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      Object existingValue = map3.get(key);
      if (existingValue instanceof Map && value instanceof Map) {
        Map<String, Object> combinedMap =
            combineMap((Map<String, Object>) existingValue, (Map<String, Object>) value);
        map3.put(key, combinedMap);
      } else {
        map3.put(key, value);
      }
    }
    return map3;
  }

  /**
   * Sub set of all configurations by some keys
   *
   * @param keys the keys you want to restrict
   * @return the subset map of the configuration
   */
  public Map<String, Object> subSet(@NotNull String... keys) {
    Map<String, Object> partial = new LinkedHashMap<>();
    if (keys.length == 0) {
      return combined;
    }
    for (String key : keys) {
      Map<String, Object> subList = toList(key, combined.get(key));
      if (subList.size() > 0) {
        partial.putAll(subList);
      } else {
        // Try to find in the tree if is a var.with.dots
        if (key.contains(".")) {
          Map<String, Object> current = traverse(key);
          partial.putAll(current);
        }
      }
    }
    partial.putAll(mainArgs);
    return partial;
  }

  /** We split dot.composed.keys looking for some value in the yaml */
  private Map<String, Object> traverse(@NotNull String key) {
    String[] keySplitted = key.split("\\.");
    Map<String, Object> current = combined;
    for (String keyS : keySplitted) {
      if (current.size() > 0) current = toList(keyS, current.get(keyS));
    }
    return current;
  }

  private Map<String, Object> toList(@NotNull String key, Object obj) {
    if (obj instanceof LinkedHashMap) {
      return (LinkedHashMap<String, Object>) obj;
    }
    Map<String, Object> list = new LinkedHashMap<>();
    if (obj != null) {
      list.put(key, obj);
    }
    return list;
  }

  /**
   * We obtain an array of --args=values that is the result of concatenated the yaml config files
   * plus main args
   *
   * @param keys to look for in the yamls
   * @return a list of --args=values
   */
  public String[] toArgs(@NotNull String... keys) {
    List<String> argList = new ArrayList<>();
    for (Entry<String, Object> conf : subSet(keys).entrySet()) {
      argList.add("--" + conf.getKey() + "=" + conf.getValue());
    }
    // This adds all combined yaml in the arg --properties
    argList.add("--properties=" + toYamlFile());
    return argList.toArray(new String[0]);
  }

  /** Replace {args} with their values, for instance {datasetId} with some value, like dr893 */
  private String substituteVars(String value, String[][] params) {
    String formatted = value;
    for (int i = 0; i < params[0].length; i++) {
      formatted = formatted.replace("{" + params[0][i] + "}", params[1][i]);
    }
    return formatted;
  }

  public Object get(String key) {
    Object value = combined.get(key);
    if (value == null && key.contains(".")) {
      // we try to traverse the tree looking for that var
      Map<String, Object> traversed = traverse(key);
      // If is an object, return the value, if not, the list of values
      return traversed.size() == 1 ? traversed.values().toArray()[0] : traversed;
    }
    return value;
  }

  @SneakyThrows
  public String toYamlFile() {
    Yaml yaml = new Yaml();
    String combinedStr = yaml.dump(combined);

    Path tempFile = Files.createTempFile(null, ".yaml");
    Files.write(tempFile, combinedStr.getBytes(), StandardOpenOption.CREATE);
    tempFile.toFile().deleteOnExit();

    return tempFile.toString();
  }
}
