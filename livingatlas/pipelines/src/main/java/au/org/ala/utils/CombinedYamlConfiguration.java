package au.org.ala.utils;

import one.util.streamex.EntryStream;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CombinedYamlConfiguration {

  private final LinkedHashMap<String, String> mainArgs = new LinkedHashMap<String, String>();
  private final String[][] mainArgsAsList;
  private LinkedHashMap<String, Object> combined = new LinkedHashMap<String, Object>();

  public CombinedYamlConfiguration(String[] mainArgs) throws FileNotFoundException {
    // First: copy main args to map
    for (String arg : mainArgs) {
      // For each arg of type --varName=value we remove the -- and split by = in varName and value
      String[] argPair = arg.replaceFirst("--", "").split("=", 2);
      // And we combine the result
      this.mainArgs.put(argPair[0], argPair[1]);
    }
    // Look for a config arg to find the yaml file paths that can be comma separated
    String config = this.mainArgs.get("config");
    if (config == null) {
      throw new RuntimeException(
          "The --config=\"some-config.yml,some-other.yml\" argument is missing");
    }
    String[] yamlConfigPaths = config.split(",");
    // we remove config, because is not an pipeline configuration
    this.mainArgs.remove("config");

    // Convert the main args to a two-dimensional Array
    mainArgsAsList =
        new String[][] {
          this.mainArgs.keySet().toArray(new String[0]),
          this.mainArgs.values().toArray(new String[0])
        };

    // Load each yaml, and combine the values
    for (String path : yamlConfigPaths) {
      InputStream input = new FileInputStream(new File(path));
      Yaml yaml = new Yaml();
      LinkedHashMap<String, Object> loaded = yaml.load(input);
      if (loaded != null) {
        // This means that config is not empty
        combined = combineMap(combined, loaded);
      }
    }
  }

  /**
   * Combine linked hash mapS.
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
  private LinkedHashMap<String, Object> combineMap(
      Map<String, Object> map1, Map<String, Object> map2) {
    Map<String, Object> map3 =
        EntryStream.of(map1)
            .append(EntryStream.of(map2))
            .toMap(
                (v1, v2) -> {
                  if (v1 instanceof Map && v2 instanceof Map) {
                    return combineMap((Map<String, Object>) v1, (Map<String, Object>) v2);
                  } else {
                    return v2;
                  }
                });
    return new LinkedHashMap<>(map3);
  }

  /**
   * Sub set of all configurations by some keys
   *
   * @param keys the keys you want to restrict
   * @return the subset map of the configuration
   */
  public LinkedHashMap<String, Object> subSet(@NotNull String... keys) {
    LinkedHashMap<String, Object> partial = new LinkedHashMap<String, Object>();
    if (keys.length == 0) {
      return combined;
    }
    for (String key : keys) {
      LinkedHashMap<String, Object> subList = toList(key, combined.get(key));
      if (subList.size() > 0) {
        partial.putAll(subList);
      } else {
        // Try to find in the tree if is a var.with.dots
        if (key.contains(".")) {
          LinkedHashMap<String, Object> current = traverse(key);
          partial.putAll(current);
        }
      }
    }
    partial.putAll(mainArgs);
    return partial;
  }

  /** We split dot.composed.keys looking for some value in the yaml */
  private LinkedHashMap<String, Object> traverse(@NotNull String key) {
    String[] keySplitted = key.split("\\.");
    LinkedHashMap<String, Object> current = combined;
    for (String keyS : keySplitted) {
      if (current.size() > 0) current = toList(keyS, current.get(keyS));
    }
    return current;
  }

  private LinkedHashMap<String, Object> toList(@NotNull String key, Object obj) {
    if (obj instanceof LinkedHashMap) {
      return (LinkedHashMap<String, Object>) obj;
    }
    LinkedHashMap<String, Object> list = new LinkedHashMap<String, Object>();
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
    return toArgs(mainArgsAsList, keys);
  }

  private String[] toArgs(String[][] params, @NotNull String... keys) {
    List<String> argList = new ArrayList<String>();
    for (Entry<String, Object> conf : subSet(keys).entrySet()) {
      Object value = format(conf.getValue(), params);
      argList.add(
          new StringBuffer()
              .append("--")
              .append(conf.getKey())
              .append("=")
              .append(value)
              .toString());
    }
    return argList.toArray(new String[0]);
  }

  /**
   * Replace {args} with their values, for instance {datasetId} with some value, like dr893
   *
   * @param value
   * @param params
   * @return
   */
  private Object format(Object value, String[][] params) {
    if (value instanceof String) {
      String formatted = (String) value;
      for (int i = 0; i < params[0].length; i++) {
        formatted = formatted.replace("{" + params[0][i] + "}", params[1][i]);
      }
      return formatted;
    } else {
      // we only format Strings, in other case we return the same object
      return value;
    }
  }

  public Object get(String key) {
    Object value = combined.get(key);
    if (value == null && key.contains(".")) {
      // we try to traverse the tree looking for that var
      LinkedHashMap<String, Object> traversed = traverse(key);
      // If is an object, return the value, if not, the list of values
      return traversed.size() == 1 ? traversed.values().toArray()[0] : traversed;
    }
    return value;
  }
}
