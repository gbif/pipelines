package org.gbif.pipelines.config;

import java.io.File;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Models a target path composed by the directory and the file name.
 */
@Deprecated
public class TargetPath implements Serializable {

  private static final long serialVersionUID = 1843400832026524345L;

  private String directory;
  private String fileName;

  // NOTE: This constructor is necessary for serialization when running on Spark with Jackson. Also, use getters and
  // setters only for properties, to avoid serialization problems.
  public TargetPath() {}

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

  public TargetPath(String fullFilePath) {
    File f = new File(fullFilePath);
    directory = f.getParentFile().getAbsolutePath();
    fileName = f.getName();
  }

  /**
   * Utility method that generates a path by concatenating the directory and the file name.
   *
   * @param directory target directory
   * @param fileName  file name
   *
   * @return path generated
   */
  public static String fullPath(String directory, String fileName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(directory),"missing directory argument");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName),"missing fileName argument");
    return directory.endsWith(File.separator) ? directory + fileName : directory + File.separator + fileName;
  }

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
    return fullPath(directory, fileName);
  }

}
