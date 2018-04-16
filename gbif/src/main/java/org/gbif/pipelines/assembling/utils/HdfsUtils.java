package org.gbif.pipelines.assembling.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to work with HDFS.
 */
public final class HdfsUtils {

  private HdfsUtils() {}

  /**
   * Returns a HDFS path for the concatenation of the values received. It creates the necessary directories if they
   * don't exist.
   */
  public static String getHdfsPath(Configuration hdfsConfig, String... values) {
    Path path = buildPath(values);
    createParentDirectory(hdfsConfig, path);
    return path.toString();
  }

  /**
   * Build a {@link Path} from an array of string values.
   */
  @VisibleForTesting
  public static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  private static void createParentDirectory(Configuration config, Path path) {
    FileSystem fs = getFileSystem(config, path);

    try {
      fs.mkdirs(path.getParent());
    } catch (IOException e) {
      throw new IllegalStateException("Error creating parent directories for extendedRepoPath: " + path.toString(), e);
    }
  }

  private static FileSystem getFileSystem(Configuration config, Path path) {
    try {
      return FileSystem.get(URI.create(path.toString()), config);
    } catch (IOException e) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + path.toString(), e);
    }
  }

}
