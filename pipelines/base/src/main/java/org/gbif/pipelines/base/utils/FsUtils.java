package org.gbif.pipelines.base.utils;

import org.gbif.pipelines.base.options.BasePipelineOptions;
import org.gbif.pipelines.parsers.exception.IORuntimeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.StringJoiner;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;

/** Utility class to work with FS. */
public final class FsUtils {

  private FsUtils() {}

  /** Build a {@link Path} from an array of string values. */
  public static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  public static String buildPathString(String... values) {
    return buildPath(values).toString();
  }

  public static String buildPath(BasePipelineOptions options, String name) {
    return FsUtils.buildPath(
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            name.toLowerCase())
        .toString();
  }

  public static String buildPathInterpret(
      BasePipelineOptions options, String directory, String uniqueId) {
    String mainPath = buildPath(options, directory);
    String fileName = "interpret-" + uniqueId;
    return FsUtils.buildPath(mainPath, fileName).toString();
  }

  public static String getTempDir(BasePipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPathString(options.getTargetPath(), "tmp")
        : options.getTempLocation();
  }

  public static String[] readArgsAsFile(String[] args) {
    if (args != null && args.length == 1) {
      String file = args[0];
      if (file.endsWith(".properties")) {
        try {
          return Files.readAllLines(Paths.get(file))
              .stream()
              .filter(x -> !Strings.isNullOrEmpty(x))
              .map(x -> x.startsWith("--") ? x : "--" + x)
              .toArray(String[]::new);
        } catch (IOException ex) {
          throw new IORuntimeException(ex);
        }
      }
    }
    return args;
  }
}
