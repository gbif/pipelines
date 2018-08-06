package org.gbif.pipelines.utils;

import org.gbif.pipelines.config.base.BaseOptions;

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

  public static String getRootPath(BaseOptions options) {
    return FsUtils.buildPathString(
        options.getTargetPath(), options.getDatasetId(), String.valueOf(options.getAttempt()));
  }

  public static String getTempDir(BaseOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPathString(options.getTargetPath(), "tmp")
        : options.getTempLocation();
  }
}
