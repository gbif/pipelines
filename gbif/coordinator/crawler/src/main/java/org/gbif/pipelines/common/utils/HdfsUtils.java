package org.gbif.pipelines.common.utils;

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;

/** Utils help to work with HDFS files */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsUtils {

  /**
   * Returns the file size in bytes
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param coreSiteConfig path to core-site.xml config file
   * @param filePath path to some file
   */
  public static long getFileSizeByte(String hdfsSiteConfig, String coreSiteConfig, String filePath)
      throws IOException {
    URI fileUri = URI.create(filePath);
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path path = new Path(fileUri);

    return fs.exists(path) ? fs.getContentSummary(path).getLength() : -1;
  }

  /**
   * Returns number of files in the directory
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param coreSiteConfig path to core-site.xml config file
   * @param directoryPath path to some directory
   */
  public static int getFileCount(String hdfsSiteConfig, String coreSiteConfig, String directoryPath)
      throws IOException {
    URI fileUri = URI.create(directoryPath);
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, directoryPath);

    int count = 0;
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(fileUri), false);
    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();
      if (fileStatus.isFile()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Checks directory
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param coreSiteConfig path to core-site.xml config file
   * @param filePath to directory
   */
  public static boolean exists(String hdfsSiteConfig, String coreSiteConfig, String filePath)
      throws IOException {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path fsPath = new Path(filePath);
    return fs.exists(fsPath);
  }

  /**
   * Returns sub directory list
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to directory
   */
  public static List<FileStatus> getSubDirList(
      String hdfsSiteConfig, String coreSiteConfig, String filePath) throws IOException {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      FileStatus[] statuses = fs.listStatus(fsPath);
      if (statuses != null && statuses.length > 0) {
        return Arrays.stream(statuses).filter(FileStatus::isDirectory).collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }

  /**
   * Reads a yaml file and returns value by key
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static String getValueByKey(
      String hdfsSiteConfig, String coreSiteConfig, String filePath, String key)
      throws IOException {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsPath)))) {
        return br.lines()
            .map(x -> x.replace("\u0000", ""))
            .filter(y -> y.startsWith(key))
            .findFirst()
            .map(z -> z.replace(key + ": ", ""))
            .orElse("");
      }
    }
    return "";
  }

  /**
   * Reads a yaml file and returns all the values
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param filePath to a yaml file
   */
  public static List<PipelineStep.MetricInfo> readMetricsFromMetaFile(
      String hdfsSiteConfig, String coreSiteConfig, String filePath) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path fsPath = new Path(filePath);
    try {
      if (fs.exists(fsPath)) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsPath)))) {
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
    StringJoiner joiner = new StringJoiner(org.apache.hadoop.fs.Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new org.apache.hadoop.fs.Path(joiner.toString());
  }

  public static String buildOutputPathAsString(String... values) {
    StringJoiner joiner = new StringJoiner(org.apache.hadoop.fs.Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return joiner.toString();
  }

  /** Delete HDFS directory */
  public static boolean deleteDirectory(
      String hdfsSiteConfig, String coreSiteConfig, String filePath) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
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
      String hdfsSiteConfig, String coreSiteConfig, String filePath, long deleteAfterDays)
      throws IOException {
    LocalDateTime date = LocalDateTime.now().minusDays(deleteAfterDays);
    getSubDirList(hdfsSiteConfig, coreSiteConfig, filePath).stream()
        .filter(
            x ->
                LocalDateTime.ofEpochSecond(x.getModificationTime(), 0, ZoneOffset.UTC)
                    .isBefore(date))
        .map(y -> y.getPath().getName())
        .forEach(z -> deleteDirectory(hdfsSiteConfig, coreSiteConfig, z));
  }

  private static FileSystem getFileSystem(
      String hdfsSiteConfig, String coreSiteConfig, String filePath) {
    return FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(filePath);
  }
}
