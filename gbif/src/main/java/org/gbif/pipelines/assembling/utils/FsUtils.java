package org.gbif.pipelines.assembling.utils;

import java.util.Arrays;
import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to work with FS.
 */
public final class FsUtils {

  private FsUtils() {}

  /**
   * Build a {@link Path} from an array of string values.
   */
  @VisibleForTesting
  public static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  public static String buildPathString(String... values) {
    return buildPath(values).toString();
  }

}
