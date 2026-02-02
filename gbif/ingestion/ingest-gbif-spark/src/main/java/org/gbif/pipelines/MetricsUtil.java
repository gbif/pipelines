package org.gbif.pipelines;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.core.utils.FsUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

@Slf4j
public class MetricsUtil {

  /**
   * @param fs
   * @param allMetrics
   * @param fileName
   */
  public static void writeMetricsYaml(
      FileSystem fs, Map<String, Long> allMetrics, String fileName) {

    // Configure YAML output (optional)
    DumperOptions options = new DumperOptions();
    options.setIndent(2);
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

    // Create YAML instance
    Yaml yaml = new Yaml(options);

    // Write to a YAML file
    try (StringWriter writer = new StringWriter()) {
      yaml.dump(allMetrics, writer);
      FsUtils.createFile(fs, fileName, writer.toString());
    } catch (IOException e) {
      log.error("Failed to write metrics yaml", e);
    }
  }
}
