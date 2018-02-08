package org.gbif.pipelines.core.config;

import java.io.File;
import java.io.Serializable;
import java.util.Optional;

/**
 * Models a target path composed by the directory and the file name.
 */
public class TargetPath implements Serializable {

  private static final long serialVersionUID = 1843400832026524345L;

  private String directory;

  private String fileName;

  /**
   * Builds a new instance using a directory and file name.
   *
   * @param dir      directory name
   * @param fileName file name
   */
  public TargetPath(String dir, String fileName) {
    directory = dir;
    this.fileName = fileName;
  }

  /**
   * Needed for json serialization with Jackson. Do NOT invoke it programmatically!! Setters are not created to
   * avoid the use of this constructor.
   */
  public TargetPath() {}

  public String getDirectory() {
    return directory;
  }

  public String getFileName() {
    return fileName;
  }

  /**
   * @return a full path which concatenates the directory and file names.
   */
  public String filePath() {
    return getFullPath(directory, fileName);
  }

  /**
   * Utility method that generates a path by concatenating the directory and the file name.
   *
   * @param directory target directory
   * @param fileName  file name
   *
   * @return path generated
   */
  public static String getFullPath(String directory, String fileName) {
    String targetDir = Optional.ofNullable(directory)
      .filter(s -> !s.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("missing directory argument"));
    String datasetId = Optional.ofNullable(fileName)
      .filter(s -> !s.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("missing fileName argument"));

    return targetDir.endsWith(File.separator) ? targetDir + datasetId : targetDir + File.separator + datasetId;
  }
}
