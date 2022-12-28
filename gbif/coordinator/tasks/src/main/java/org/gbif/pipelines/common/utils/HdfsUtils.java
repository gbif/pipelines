package org.gbif.pipelines.common.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

/** Utils help to work with HDFS files */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsUtils {

  /**
   * Returns the file size in bytes
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath path to some file
   */
  public static long getFileSizeByte(HdfsConfigs hdfsConfigs, String filePath) throws IOException {
    URI fileUri = URI.create(filePath);
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
    Path path = new Path(fileUri);

    return fs.exists(path) ? fs.getContentSummary(path).getLength() : -1;
  }

  /**
   * @param filePath
   * @param fs
   * @return list of absolute file path present in given path
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<FileStatus> getAllFilesRecursive(Path filePath, FileSystem fs)
      throws FileNotFoundException, IOException {
    List<FileStatus> fileList = new ArrayList<>();
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDir()) {
        fileList.addAll(getAllFilesRecursive(fileStat.getPath(), fs));
      } else {
        fileList.add(fileStat);
      }
    }
    return fileList;
  }

  /**
   * Returns number of files in the directory
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param directoryPath path to some directory
   */
  public static int getFileCount(HdfsConfigs hdfsConfigs, String directoryPath) throws IOException {
    URI fileUri = URI.create(directoryPath);
    FileSystem fs = getFileSystem(hdfsConfigs, directoryPath);

    return getAllFilesRecursive(new Path(fileUri), fs).size();
  }

  /**
   * Checks directory
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath to directory
   */
  public static boolean exists(HdfsConfigs hdfsConfigs, String filePath) throws IOException {
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
    Path fsPath = new Path(filePath);
    return fs.exists(fsPath);
  }

  /**
   * Returns sub directory list
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath to directory
   */
  public static List<FileStatus> getSubDirList(HdfsConfigs hdfsConfigs, String filePath)
      throws IOException {
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      FileStatus[] statuses = fs.listStatus(fsPath);
      if (statuses != null && statuses.length > 0) {
        return Arrays.stream(statuses).filter(FileStatus::isDir).collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }

  /**
   * Reads a yaml file and returns value by key
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static Optional<String> getValueByKey(HdfsConfigs hdfsConfigs, String filePath, String key)
      throws IOException {
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsPath), UTF_8))) {
        return br.lines()
            .map(x -> x.replace("\u0000", ""))
            .filter(y -> y.startsWith(key))
            .findFirst()
            .map(z -> z.replace(key + ": ", ""));
      }
    }
    return Optional.empty();
  }

  /**
   * Reads a yaml file and returns long by key
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static Optional<Long> getLongByKey(HdfsConfigs hdfsConfigs, String filePath, String key)
      throws IOException {
    return getValueByKey(hdfsConfigs, filePath, key).map(Long::parseLong);
  }

  /**
   * Reads a yaml file and returns double by key
   *
   * @param hdfsConfigs path to hdfs-site.xml and core-site.xml config file
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static Optional<Double> getDoubleByKey(
      HdfsConfigs hdfsConfigs, String filePath, String key) throws IOException {
    return getValueByKey(hdfsConfigs, filePath, key).map(Double::parseDouble);
  }

  /**
   * Reads a yaml file and returns all the values
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param filePath to a yaml file
   */
  public static List<PipelineStep.MetricInfo> readMetricsFromMetaFile(
      HdfsConfigs hdfsConfigs, String filePath) {
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
    Path fsPath = new Path(filePath);
    try {
      if (fs.exists(fsPath)) {
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(fs.open(fsPath), UTF_8))) {
          return br.lines()
              .map(x -> x.replace("\u0000", ""))
              .filter(s -> !Strings.isNullOrEmpty(s))
              .map(z -> z.split(":"))
              .filter(s -> s.length > 1)
              .map(v -> new PipelineStep.MetricInfo(v[0].trim(), v[1].trim()))
              .collect(Collectors.toList());
        }
      }
    } catch (IOException e) {
      log.warn("Couldn't read meta file from {}", filePath, e);
    }
    return new ArrayList<>();
  }

  /** Store an Avro file on HDFS in /data/ingest/<datasetUUID>/<attemptID>/verbatim.avro */
  public static Path buildOutputPath(String... values) {
    return new org.apache.hadoop.fs.Path(buildOutputPathAsString(values));
  }

  public static String buildOutputPathAsString(String... values) {
    return String.join(org.apache.hadoop.fs.Path.SEPARATOR, values);
  }

  /** Delete HDFS directory */
  public static boolean deleteDirectory(HdfsConfigs hdfsConfigs, String filePath) {
    FileSystem fs = getFileSystem(hdfsConfigs, filePath);
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

  /** Delete HDFS sub-directories where modification date is older than deleteAfterDays value */
  public static void deleteSubFolders(
      HdfsConfigs hdfsConfigs, String filePath, long deleteAfterDays, Set<String> exclude)
      throws IOException {

    LocalDateTime limitDate = LocalDateTime.now().minusDays(deleteAfterDays);

    getSubDirList(hdfsConfigs, filePath).stream()
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
              boolean deleted = deleteDirectory(hdfsConfigs, attemptToDelete);
              log.info("Tried to delete directory {}, is deleted? {}", attemptToDelete, deleted);
            });
  }

  private static FileSystem getFileSystem(HdfsConfigs hdfsConfigs, String filePath) {
    return FileSystemFactory.getInstance(hdfsConfigs).getFs(filePath);
  }
}
