package org.gbif.pipelines.assembling.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class HdfsUtils {

  private HdfsUtils() {}

  public static String getHdfsPath(Configuration hdfsConfig, String...values) {
    Path path = buildPath(values);
    createParentDirectory(hdfsConfig, path);
    return path.toString();
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

  private static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

}
