package org.gbif.pipelines.esindexing.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Utility class to work with files.
 */
public final class FileUtils {

  private FileUtils() {}

  /**
   * Loads a file from a path. It works for both absolute and relative paths.
   */
  public static InputStream loadFile(Path path) {
    return path.isAbsolute() ? loadAbsoluteFile(path) : loadRelativePath(path);
  }

  private static InputStream loadAbsoluteFile(Path path) {
    try {
      return new FileInputStream(path.toFile());
    } catch (FileNotFoundException exc) {
      throw new IllegalArgumentException(exc.getMessage(), exc);
    }
  }

  private static InputStream loadRelativePath(Path path) {
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());
  }

}
