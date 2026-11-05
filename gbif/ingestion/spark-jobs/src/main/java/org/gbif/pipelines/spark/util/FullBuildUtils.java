package org.gbif.pipelines.spark.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class FullBuildUtils {

  public static final String MISSING = "MISSING";
  public static final String OUT_OF_SYNC = "OUT_OF_SYNC";

  /** How often (in dataset directories) to log scan progress. */
  private static final int SCAN_PROGRESS_INTERVAL = 1000;

  public static void checkDatasetTypeSupported(DatasetType datasetType) {
    if (datasetType != DatasetType.OCCURRENCE && datasetType != DatasetType.SAMPLING_EVENT) {
      throw new IllegalArgumentException("Invalid dataset type: " + datasetType);
    }
  }

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
    if (fileStatuses == null) {
      throw new IOException("Failed to list directories in " + config.getOutputPath());
    }

    List<String> unsuccessfulDatasets = new ArrayList<>();
    List<String> tooOldDatasets = new ArrayList<>();
    Map<String, Integer> datasetAttemptMap = new java.util.HashMap<>();

    // for each directory, find the last successful interpretation
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        String datasetId = fileStatus.getPath().getName();

        FileStatus newestSuccessFile =
            newestSuccessFile(fileSystem, fileStatus.getPath(), sourceDirectory);

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
      }
    }

    // write unsuccessful datasets to a hdfs file for later review
    Path unsuccessfulFilePath = new Path(unsuccessfulFileDumpPath);
    try (org.apache.hadoop.fs.FSDataOutputStream outputStream =
        fileSystem.create(unsuccessfulFilePath, true)) {
      for (String datasetId : unsuccessfulDatasets) {
        outputStream.writeBytes(datasetId + "," + MISSING + "\n");
      }
      for (String datasetId : tooOldDatasets) {
        outputStream.writeBytes(datasetId + "," + OUT_OF_SYNC + "\n");
      }
    }

    return new DirectoryScanResult(hdfsPaths, datasetAttemptMap);
  }

  /**
   * Attempt of the most recent successful interpretation of a dataset, the one with the newest
   * _SUCCESS file in its source directory, as {@link #getSuccessfulParquetFilePaths} picks it
   */
  public static Optional<Integer> latestSuccessfulAttempt(
      FileSystem fileSystem, PipelinesConfig config, String datasetId, String sourceDirectory)
      throws IOException {
    FileStatus newestSuccessFile =
        newestSuccessFile(
            fileSystem, new Path(config.getOutputPath() + "/" + datasetId), sourceDirectory);
    if (newestSuccessFile == null) {
      return Optional.empty();
    }
    // go up from <sourceDirectory>/_SUCCESS to the attempt directory
    String attempt = newestSuccessFile.getPath().getParent().getParent().getName();
    try {
      return Optional.of(Integer.parseInt(attempt));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /** Newest _SUCCESS file in the source directory of the attempts of a dataset, or null */
  private static FileStatus newestSuccessFile(
      FileSystem fileSystem, Path datasetDirectory, String sourceDirectory) throws IOException {
    // look for _SUCCESS files in the subdirectories of the dataset directory
    FileStatus[] successFiles =
        fileSystem.globStatus(new Path(datasetDirectory + "/*/" + sourceDirectory + "/_SUCCESS"));

    FileStatus newestSuccessFile = null;
    if (successFiles != null) {
      for (FileStatus successFile : successFiles) {
        if (newestSuccessFile == null
            || successFile.getModificationTime() > newestSuccessFile.getModificationTime()) {
          newestSuccessFile = successFile;
        }
      }
    }
    return newestSuccessFile;
  }

  /**
   * Scans the output directory (e.g. /data/ingest) for a given file (e.g.
   * verbatim-to-identifier.yml) in each dataset's attempt directories, i.e. {@code
   * <outputPath>/<datasetId>/<attempt>/<relativeFilePath>}. For each dataset the newest matching
   * file (by modification time) determines the latest successful attempt.
   *
   * <p>Datasets without a matching file, or whose attempt directory isn't numeric, are skipped.
   *
   * @param fileSystem the Hadoop FileSystem to use for scanning directories
   * @param config the PipelinesConfig containing the output path to scan
   * @param relativeFilePath the file to look for, relative to the attempt directory
   * @return a DirectoryScanResult where successfulPaths holds the full path of each matched file,
   *     and datasetAttemptMap maps datasetId to the attempt number the file was found in
   */
  public static @NotNull DirectoryScanResult getLatestAttemptFilePaths(
      FileSystem fileSystem, PipelinesConfig config, String relativeFilePath) throws IOException {

    long start = System.currentTimeMillis();
    log.info("Listing dataset directories in {}", config.getOutputPath());
    FileStatus[] datasetDirs = fileSystem.globStatus(new Path(config.getOutputPath() + "/*"));
    if (datasetDirs == null) {
      throw new IOException("Failed to list directories in " + config.getOutputPath());
    }
    log.info(
        "Found {} entries in {}, scanning for {} (listing took {} ms)",
        datasetDirs.length,
        config.getOutputPath(),
        relativeFilePath,
        System.currentTimeMillis() - start);

    List<String> filePaths = new ArrayList<>();
    Map<String, Integer> datasetAttemptMap = new java.util.HashMap<>();
    int scanned = 0;
    int withoutFile = 0;
    int nonNumericAttempt = 0;

    for (FileStatus datasetDir : datasetDirs) {
      scanned++;
      if (scanned % SCAN_PROGRESS_INTERVAL == 0) {
        long elapsedMs = System.currentTimeMillis() - start;
        log.info(
            "Scan progress: {}/{} directories ({}%), {} with {}, {} without, elapsed {} s",
            scanned,
            datasetDirs.length,
            scanned * 100 / datasetDirs.length,
            datasetAttemptMap.size(),
            relativeFilePath,
            withoutFile,
            elapsedMs / 1000);
      }
      if (!datasetDir.isDirectory()) {
        continue;
      }
      String datasetId = datasetDir.getPath().getName();

      FileStatus[] matches =
          fileSystem.globStatus(new Path(datasetDir.getPath() + "/*/" + relativeFilePath));
      if (matches == null || matches.length == 0) {
        withoutFile++;
        log.debug("Dataset {} has no {}", datasetId, relativeFilePath);
        continue;
      }

      FileStatus newest = null;
      for (FileStatus match : matches) {
        if (newest == null || match.getModificationTime() > newest.getModificationTime()) {
          newest = match;
        }
      }

      // walk up from <attempt>/<relativeFilePath> to the attempt directory
      Path attemptDir = newest.getPath().getParent();
      int depth = new Path(relativeFilePath).depth();
      for (int i = 1; i < depth; i++) {
        attemptDir = attemptDir.getParent();
      }

      try {
        datasetAttemptMap.put(datasetId, Integer.parseInt(attemptDir.getName()));
        filePaths.add(newest.getPath().toString());
        log.debug(
            "Dataset {}: latest {} in attempt {} ({} attempts have it)",
            datasetId,
            relativeFilePath,
            attemptDir.getName(),
            matches.length);
      } catch (NumberFormatException e) {
        // unexpected directory structure - skip this dataset
        nonNumericAttempt++;
        log.warn("Skipping {}: attempt directory isn't numeric", newest.getPath());
      }
    }

    log.info(
        "Scan complete: {} directories, {} with {}, {} without, {} skipped (non-numeric attempt)"
            + " in {} s",
        datasetDirs.length,
        datasetAttemptMap.size(),
        relativeFilePath,
        withoutFile,
        nonNumericAttempt,
        (System.currentTimeMillis() - start) / 1000);

    return new DirectoryScanResult(filePaths, datasetAttemptMap);
  }

  public record DirectoryScanResult(
      List<String> successfulPaths, Map<String, Integer> datasetAttemptMap)
      implements Serializable {}
}
