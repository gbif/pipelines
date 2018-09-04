package org.gbif.pipelines.base.utils;

import org.gbif.pipelines.base.options.base.BaseOptions;

import java.util.Arrays;
import java.util.StringJoiner;

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

  public static String buildPath(BaseOptions options, String name) {
    return FsUtils.buildPath(
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            name.toLowerCase())
        .toString();
  }

  public static String buildPathInterpret(BaseOptions options, String directory, String uniqueId) {
    String mainPath = buildPath(options, directory);
    String fileName = "interpret-" + uniqueId;
    return FsUtils.buildPath(mainPath, fileName).toString();
  }
}
