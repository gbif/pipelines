package org.gbif.pipelines.core.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesException;
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
   * Reads metrics YAML from FS (HDFS if configured). Files may be UTF-8 or UTF-16 (with or without
   * a byte order mark) - {@link #writeMetricsYaml} writes UTF-16BE without a BOM via {@code
   * writeChars}, while other writers produce UTF-8. See {@link #decodeYaml(byte[])}.
   *
   * @param fs the filesystem
   * @param fileName the name of the file to read the metrics from
   * @return the metrics map, or empty map if the file cannot be read
   */
  public static Map<String, Long> readMetricsYaml(FileSystem fs, String fileName) {
    try (InputStream in = fs.open(new Path(fileName))) {

      Map<String, Object> raw = new Yaml().load(decodeYaml(in.readAllBytes()));
      if (raw == null || raw.isEmpty()) {
        return Map.of();
      }

      Map<String, Long> result = new HashMap<>(raw.size());
      for (Map.Entry<String, Object> e : raw.entrySet()) {
        Object value = e.getValue();
        if (value instanceof Number) {
          result.put(e.getKey(), ((Number) value).longValue());
        } else if (value != null) {
          log.warn("Skipping non-numeric metric {} = {} in {}", e.getKey(), value, fileName);
        }
      }
      return result;
    } catch (Exception e) {
      throw new PipelinesException("Failed to read metrics yaml from " + fileName, e);
    }
  }

  /**
   * Decodes the bytes of a metrics YAML file, detecting UTF-8 or UTF-16 (big or little endian).
   *
   * <p>A byte order mark is used when present and stripped. Otherwise UTF-16 is detected from the
   * zero bytes ASCII characters produce in it: the metrics files are plain ASCII keys and numbers,
   * so a leading {@code 00 xx} pair means UTF-16BE and {@code xx 00} means UTF-16LE. Anything else
   * is treated as UTF-8.
   */
  public static String decodeYaml(byte[] bytes) {
    Charset charset = detectCharset(bytes);
    int bomLength = bomLength(bytes);
    return new String(bytes, bomLength, bytes.length - bomLength, charset);
  }

  static Charset detectCharset(byte[] b) {
    if (b.length >= 3 && (b[0] & 0xFF) == 0xEF && (b[1] & 0xFF) == 0xBB && (b[2] & 0xFF) == 0xBF) {
      return StandardCharsets.UTF_8;
    }
    if (b.length >= 2 && (b[0] & 0xFF) == 0xFE && (b[1] & 0xFF) == 0xFF) {
      return StandardCharsets.UTF_16BE;
    }
    if (b.length >= 2 && (b[0] & 0xFF) == 0xFF && (b[1] & 0xFF) == 0xFE) {
      return StandardCharsets.UTF_16LE;
    }
    if (b.length >= 2 && b[0] == 0 && b[1] != 0) {
      return StandardCharsets.UTF_16BE;
    }
    if (b.length >= 2 && b[0] != 0 && b[1] == 0) {
      return StandardCharsets.UTF_16LE;
    }
    return StandardCharsets.UTF_8;
  }

  private static int bomLength(byte[] b) {
    if (b.length >= 3 && (b[0] & 0xFF) == 0xEF && (b[1] & 0xFF) == 0xBB && (b[2] & 0xFF) == 0xBF) {
      return 3;
    }
    if (b.length >= 2
        && (((b[0] & 0xFF) == 0xFE && (b[1] & 0xFF) == 0xFF)
            || ((b[0] & 0xFF) == 0xFF && (b[1] & 0xFF) == 0xFE))) {
      return 2;
    }
    return 0;
  }
}
