package org.gbif.pipelines.base.utils;

import org.gbif.pipelines.base.options.base.BaseOptions;
import org.gbif.pipelines.core.RecordType;

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

  public static String buildPath(BaseOptions options, RecordType type) {
    return FsUtils.buildPath(
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            type.name().toLowerCase())
        .toString();
  }

  public static String buildPathInterpret(BaseOptions options, RecordType type) {
    return FsUtils.buildPath(buildPath(options, type), "interpret").toString();
  }
}
