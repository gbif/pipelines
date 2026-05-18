package org.gbif.pipelines.coordinator;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class CleanupUtil {

  /**
   * Routine to clean up HDFS previous attempts
   *
   * @param config
   * @param fileSystem
   * @param datasetId
   * @param exclude
   * @throws Exception
   */
  public static void cleanupPreviousOnSuccess(
      PipelinesConfig config, FileSystem fileSystem, String datasetId, Set<String> exclude) {

    try {
      log.debug("Deleting old attempts directories");
      String pathToDelete = String.join("/", config.getInputPath(), datasetId);

      LocalDateTime limitDate = LocalDateTime.now().minusDays(config.getDeleteAfterDays());

      getSubDirList(fileSystem, pathToDelete).stream()
          .filter(x -> !exclude.contains(x.getPath().getName()))
          .filter(
              fileStatus ->
                  LocalDateTime.ofInstant(
                          Instant.ofEpochMilli(fileStatus.getModificationTime()),
                          ZoneId.systemDefault())
                      .isBefore(limitDate))
          .map(fileStatus -> fileStatus.getPath().toString())
          .forEach(
              attemptToDelete -> {
                boolean deleted = deleteDirectory(fileSystem, attemptToDelete);
                log.info("Tried to delete directory {}, is deleted? {}", attemptToDelete, deleted);
              });
    } catch (IOException ex) {
      log.error("Failed to delete old attempts", ex);
    }
  }

  public static boolean deleteDirectory(FileSystem fs, String filePath) {
    Path fsPath = new Path(filePath);
    try {
      if (fs.exists(fsPath)) {
        return fs.delete(fsPath, true);
      }
    } catch (IOException ex) {
      throw new IllegalStateException("Exception during deletion " + filePath, ex);
    }

    return true;
  }

  /**
   * Returns sub directory list
   *
   * @param filePath to directory
   */
  public static List<FileStatus> getSubDirList(FileSystem fs, String filePath) throws IOException {
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      FileStatus[] statuses = fs.listStatus(fsPath);
      if (statuses != null && statuses.length > 0) {
        return Arrays.stream(statuses).filter(FileStatus::isDirectory).collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }
}
