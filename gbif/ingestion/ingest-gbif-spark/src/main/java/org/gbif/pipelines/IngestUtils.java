package org.gbif.pipelines;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

public class IngestUtils {

  /**
   * Scans the output directory (e.g. /data/ingest) for _SUCCESS files to find the most recent
   * successful interpretation for each dataset, and returns the paths to read from as well as a map
   * of datasetId to attempt number. Also writes the list of unsuccessful datasets to a hdfs file
   * for later review.
   *
   * @param fileSystem the Hadoop FileSystem to use for scanning directories and writing the
   *     unsuccessful datasets file
   * @param config the PipelinesConfig containing the output path to scan
   * @param sourceDirectory the name of the source directory to look for (e.g. "hdfs" or "json")
   * @param unsuccessfulFileDumpPath the HDFS path to write the list of unsuccessful datasets to
   *     (e.g. /data/unsuccessful_datasets.txt)
   * @return a DirectoryScanResult containing the list of successful paths to read from and a map of
   *     datasetId to attempt number
   * @throws IOException
   */
  public static @NotNull DirectoryScanResult getSuccessfulParquetFilePaths(
      FileSystem fileSystem,
      PipelinesConfig config,
      String sourceDirectory,
      String unsuccessfulFileDumpPath,
      String earliestAllowedSuccessFileDateString)
      throws IOException {

    Long earliestAllowedSuccessFileDateEpochMillis = null;

    if (StringUtils.isNotBlank(earliestAllowedSuccessFileDateString)) {

      // parse date ISO 8601 format to epoch seconds
      earliestAllowedSuccessFileDateEpochMillis =
          java.time.Instant.parse(earliestAllowedSuccessFileDateString).toEpochMilli();
    }

    List<String> hdfsPaths = new ArrayList<>();
    FileStatus[] fileStatuses = fileSystem.globStatus(new Path(config.getOutputPath() + "/*"));
    List<String> unsuccessfulDatasets = new ArrayList<>();
    List<String> tooOldDatasets = new ArrayList<>();
    Map<String, Integer> datasetAttemptMap = new java.util.HashMap<>();

    // for each directory, find the last successful interpretation and create a symlink to it
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetId = fileStatus.getPath().getName();

        // look for _SUCCESS files in the subdirectories of the dataset directory
        FileStatus[] successFiles =
            fileSystem.globStatus(
                new Path(fileStatus.getPath() + "/*/" + sourceDirectory + "/_SUCCESS"));

        if (successFiles != null && successFiles.length > 0) {
          // find the newest _SUCCESS file
          FileStatus newestSuccessFile = null;
          for (FileStatus successFile : successFiles) {
            if (newestSuccessFile == null
                || successFile.getModificationTime() > newestSuccessFile.getModificationTime()) {
              newestSuccessFile = successFile;
            }
          }

          if (newestSuccessFile != null) {

            if (earliestAllowedSuccessFileDateEpochMillis != null
                && newestSuccessFile.getModificationTime()
                    < earliestAllowedSuccessFileDateEpochMillis) {
              tooOldDatasets.add(datasetId);
            } else {

              Path successAttemptDir =
                  newestSuccessFile
                      .getPath()
                      .getParent()
                      .getParent(); // go up from hdfs/_SUCCESS to interpretation dir

              String attempt = successAttemptDir.getName(); // this should be the attempt number

              try {
                // add if the _SUCCESS file is less than 4 weeks old to the list of paths to read
                // from
                datasetAttemptMap.put(datasetId, Integer.parseInt(attempt));
                hdfsPaths.add(successAttemptDir + "/" + sourceDirectory + "/");
              } catch (NumberFormatException e) {
                // ignore - this may happen if the directory structure is not as expected,
                // in which case we just skip this dataset
                unsuccessfulDatasets.add(datasetId);
              }
            }
          } else {
            unsuccessfulDatasets.add(datasetId);
          }
        } else {
          unsuccessfulDatasets.add(datasetId);
        }
      }
    }

    // write unsuccessful datasets to a hdfs file for later review
    Path unsuccessfulFilePath = new Path(unsuccessfulFileDumpPath);
    try (org.apache.hadoop.fs.FSDataOutputStream outputStream =
        fileSystem.create(unsuccessfulFilePath)) {
      for (String datasetId : unsuccessfulDatasets) {
        outputStream.writeBytes(datasetId + ",MISSING\n");
      }
      for (String datasetId : tooOldDatasets) {
        outputStream.writeBytes(datasetId + ",OUT_OF_SYNC\n");
      }
    }

    return new DirectoryScanResult(hdfsPaths, datasetAttemptMap);
  }

  public record DirectoryScanResult(
      List<String> successfulPaths, Map<String, Integer> datasetAttemptMap)
      implements Serializable {}
}
