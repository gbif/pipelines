package org.gbif.pipelines.spark.util;

public class PathUtil {

  /**
   * Frictionless datapackage path as stored by the crawler. </br> <bold>Format:</bold>
   * {base}/{datasetKey}/{datasetKey}.{attempt}
   */
  public static String crawlAttemptPath(String base, String datasetId, int attempt) {
    return String.format("%s/%s/%s.%d", stripTrailingSlash(base), datasetId, datasetId, attempt);
  }

  /** Pipeline interpreted output path. </br> <bold>Format:</bold> {base}/{datasetKey}/{attempt} */
  public static String interpretedAttemptPath(String base, String datasetId, int attempt) {
    return String.format("%s/%s/%d", stripTrailingSlash(base), datasetId, attempt);
  }

  private static String stripTrailingSlash(String path) {
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }
}
