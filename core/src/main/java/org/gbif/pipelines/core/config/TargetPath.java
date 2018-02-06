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

  public TargetPath() {}

  public TargetPath(String dir, String fileName) {
    this.directory = dir;
    this.fileName = fileName;
  }

  public TargetPath(String fullFilePath) {
    File f = new File(fullFilePath);
    this.directory = f.getParentFile().getAbsolutePath();
    this.fileName = f.getName();
  }

  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFullPath() {
    return TargetPath.getFullPath(this.getDirectory(), this.getFileName());
  }
}
