package org.gbif.pipelines.core.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

@Slf4j
public class MetricsUtil {

  /**
   * Writes the metrics YAML to FS (HDFS if configured)
   *
   * @param fs the filesystem
   * @param metrics the metrics to serialise
   * @param fileName the name of the file to write the metrics to
   */
  public static void writeMetricsYaml(FileSystem fs, Map<String, Long> metrics, String fileName) {
    DumperOptions options = new DumperOptions();
    options.setIndent(2);
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

    Yaml yaml = new Yaml(options);

    try (StringWriter writer = new StringWriter()) {
      yaml.dump(metrics, writer);
      log.debug("Writing metrics to yml file {}", fileName);
      FsUtils.createFile(fs, fileName, writer.toString());
    } catch (IOException e) {
      log.error("Failed to write metrics yaml to {}", fileName, e);
    }
  }

  /**
   * Reads metrics YAML from FS (HDFS if configured)
   *
   * @param fs the filesystem
   * @param fileName the name of the file to read the metrics from
   * @return the metrics map, or empty map if the file cannot be read
   */
  public static Map<String, Long> readMetricsYaml(FileSystem fs, String fileName) {
    try (InputStream in = fs.open(new Path(fileName));
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
      return new Yaml().load(reader);
    } catch (Exception e) {
      log.error("Failed to read metrics yaml from {}", fileName, e);
      return Map.of();
    }
  }
}
