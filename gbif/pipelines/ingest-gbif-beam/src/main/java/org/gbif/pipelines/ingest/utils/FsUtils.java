package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

/** Utility class to work with file system. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FsUtils {

  /** Build a {@link Path} from an array of string values using path separator. */
  public static Path buildPath(String... values) {
    return new Path(String.join(Path.SEPARATOR, values));
  }

  /**
   * Uses pattern for path - "{targetPath}/{datasetId}/{attempt}/{name}"
   *
   * @return string path
   */
  public static String buildDatasetAttemptPath(
      BasePipelineOptions options, String name, boolean isInput) {
    return FsUtils.buildPath(
            isInput ? options.getInputPath() : options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            name.toLowerCase())
        .toString();
  }

  /**
   * Uses pattern for path -
   * "{targetPath}/{datasetId}/{attempt}/interpreted/{name}/interpret-{uniqueId}"
   *
   * @return string path to interpretation
   */
  public static String buildPathInterpretUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, false),
            name,
            Interpretation.FILE_NAME + uniqueId)
        .toString();
  }

  /**
   * Uses pattern for path -
   * "{targetPath}/{datasetId}/{attempt}/interpreted/{name}/interpret-{uniqueId}"
   *
   * @return string path to interpretation
   */
  public static String buildPathInterpretUsingInputPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, true),
            name,
            Interpretation.FILE_NAME + uniqueId)
        .toString();
  }

  /**
   * Builds the target base path of the Occurrence hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the occurrence hdfs view is stored
   */
  public static String buildFilePathHdfsViewUsingInputPath(
      BasePipelineOptions options, String uniqueId) {
    return FsUtils.buildPath(
            buildPathHdfsViewUsingInputPath(options), HdfsView.VIEW_OCCURRENCE + "_" + uniqueId)
        .toString();
  }

  /**
   * Builds the target base path of the Occurrence hdfs view.
   *
   * @param options options pipeline options
   * @return path to the directory where the occurrence hdfs view is stored
   */
  public static String buildPathHdfsViewUsingInputPath(BasePipelineOptions options) {
    return FsUtils.buildPath(
            buildDatasetAttemptPath(options, DIRECTORY_NAME, true),
            OCCURRENCE_HDFS_RECORD.name().toLowerCase())
        .toString();
  }

  /**
   * Gets temporary directory from options or returns default value.
   *
   * @return path to a temporary directory.
   */
  public static String getTempDir(BasePipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPath(options.getTargetPath(), "tmp").toString()
        : options.getTempLocation();
  }

  /**
   * Reads Beam options from arguments or file.
   *
   * @return array of Beam arguments.
   */
  @SneakyThrows
  public static String[] readArgsAsFile(String[] args) {
    if (args == null || args.length != 1) {
      return args;
    }

    String file = args[0];
    if (!file.endsWith(".properties")) {
      return args;
    }

    return Files.readAllLines(Paths.get(file)).stream()
        .filter(x -> !Strings.isNullOrEmpty(x))
        .map(x -> x.startsWith("--") ? x : "--" + x)
        .toArray(String[]::new);
  }

  /** Removes temporal directory, before closing Main thread */
  public static void removeTmpDirectory(BasePipelineOptions options) {
    Runnable runnable =
        () -> {
          File tmp = Paths.get(getTempDir(options)).toFile();
          if (tmp.exists()) {
            try {
              FileUtils.deleteDirectory(tmp);
              log.info("temp directory {} deleted", tmp.getPath());
            } catch (IOException e) {
              log.error("Could not delete temp directory {}", tmp.getPath());
            }
          }
        };

    Runtime.getRuntime().addShutdownHook(new Thread(runnable));
  }

  /** Helper method to get file system based on provided configuration. */
  @SneakyThrows
  public static FileSystem getFileSystem(
      String hdfsSiteConfig, String coreSiteConfig, String path) {
    return FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(path);
  }

  /** Helper method to get file system based on provided configuration. */
  @SneakyThrows
  public static FileSystem getLocalFileSystem(String hdfsSiteConfig, String coreSiteConfig) {
    return FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getLocalFs();
  }

  /** Helper method to write/overwrite a file */
  public static void createFile(FileSystem fs, String path, String body) throws IOException {
    try (FSDataOutputStream stream = fs.create(new Path(path), true)) {
      stream.writeChars(body);
    }
  }

  /**
   * Deletes all directories and subdirectories(recursively) by file prefix name.
   *
   * <p>Example: all directories with '.temp-' prefix in directory
   * '89aad0bb-654f-483c-8711-2c00551033ae/3'
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param directoryPath to a directory
   * @param filePrefix file name prefix
   */
  public static void deleteDirectoryByPrefix(
      String hdfsSiteConfig, String coreSiteConfig, String directoryPath, String filePrefix) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, directoryPath);
    try {
      deleteDirectoryByPrefix(fs, new Path(directoryPath), filePrefix);
    } catch (IOException e) {
      log.warn("Can't delete folder - {}, prefix - {}", directoryPath, filePrefix);
    }
  }

  /**
   * Moves a list files that match against a glob filter into a target directory.
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param globFilter filter used to filter files and paths
   * @param targetPath target directory
   */
  public static void moveDirectory(
      String hdfsSiteConfig, String coreSiteConfig, String targetPath, String globFilter) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, targetPath);
    try {
      FileStatus[] status = fs.globStatus(new Path(globFilter));
      Path[] paths = FileUtil.stat2Paths(status);
      for (Path path : paths) {
        boolean rename = fs.rename(path, new Path(targetPath, path.getName()));
        log.info("File {} moved status - {}", path.toString(), rename);
      }
    } catch (IOException e) {
      log.warn("Can't move files using filter - {}, into path - {}", globFilter, targetPath);
    }
  }

  /**
   * Deletes a list files that match against a glob filter into a target directory.
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param globFilter filter used to filter files and paths
   */
  public static void deleteByPattern(
      String hdfsSiteConfig, String coreSiteConfig, String directoryPath, String globFilter) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, directoryPath);
    try {
      FileStatus[] status = fs.globStatus(new Path(globFilter));
      Path[] paths = FileUtil.stat2Paths(status);
      for (Path path : paths) {
        fs.delete(path, Boolean.TRUE);
      }
    } catch (IOException e) {
      log.warn("Can't delete files using filter - {}", globFilter);
    }
  }

  private static void deleteDirectoryByPrefix(FileSystem fs, Path directoryPath, String filePrefix)
      throws IOException {
    FileStatus[] status = fs.listStatus(directoryPath);
    List<Path> list =
        Arrays.stream(status)
            .filter(FileStatus::isDirectory)
            .map(FileStatus::getPath)
            .collect(Collectors.toList());

    for (Path dir : list) {
      if (dir.getName().startsWith(filePrefix)) {
        fs.delete(dir, true);
      } else {
        deleteDirectoryByPrefix(fs, dir, filePrefix);
      }
    }
  }

  /**
   * Removes a directory with content if the folder exists
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param directoryPath path to some directory
   */
  public static boolean deleteIfExist(
      String hdfsSiteConfig, String coreSiteConfig, String directoryPath) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, directoryPath);

    Path path = new Path(directoryPath);
    try {
      return fs.exists(path) && fs.delete(path, true);
    } catch (IOException e) {
      log.error("Can't delete {} directory, cause - {}", directoryPath, e.getCause());
      return false;
    }
  }

  /**
   * Read a properties file from HDFS/Local FS
   *
   * @param hdfsSiteConfig HDFS config file
   * @param filePath properties file path
   */
  @SneakyThrows
  public static <T> T readConfigFile(
      String hdfsSiteConfig, String coreSiteConfig, String filePath, Class<T> clazz) {
    FileSystem fs = FsUtils.getLocalFileSystem(hdfsSiteConfig, coreSiteConfig);
    Path fPath = new Path(filePath);
    if (fs.exists(fPath)) {
      log.info("Reading properties path - {}", filePath);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath)))) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        mapper.findAndRegisterModules();
        return mapper.readValue(br, clazz);
      }
    }
    throw new FileNotFoundException("The properties file doesn't exist - " + filePath);
  }

  /** Deletes directories if a dataset with the same attempt was interpreted before */
  public static void deleteInterpretIfExist(
      String hdfsSiteConfig,
      String coreSiteConfig,
      String basePath,
      String datasetId,
      Integer attempt,
      Set<String> steps) {
    if (steps != null && !steps.isEmpty()) {

      String path = String.join("/", basePath, datasetId, attempt.toString(), DIRECTORY_NAME);

      if (steps.contains(ALL.name())) {
        log.info("Delete interpretation directory - {}", path);
        boolean isDeleted = deleteIfExist(hdfsSiteConfig, coreSiteConfig, path);
        log.info("Delete interpretation directory - {}, deleted - {}", path, isDeleted);
      } else {
        for (String step : steps) {
          log.info("Delete {}/{} directory", path, step.toLowerCase());
          boolean isDeleted =
              deleteIfExist(
                  hdfsSiteConfig, coreSiteConfig, String.join("/", path, step.toLowerCase()));
          log.info("Delete interpretation directory - {}, deleted - {}", path, isDeleted);
        }
      }
    }
  }

  /**
   * Copies all occurrence records into the directory from targetPath. Deletes pre-existing data of
   * the dataset being processed.
   */
  public static void copyOccurrenceRecords(InterpretationPipelineOptions options) {
    // Moving files to the directory of latest records
    String targetPath = options.getTargetPath();

    String deletePath =
        FsUtils.buildPath(
                targetPath, HdfsView.VIEW_OCCURRENCE + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);
    String filter = buildFilePathHdfsViewUsingInputPath(options, "*.avro");

    log.info("Moving files with pattern {} to {}", filter, targetPath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, filter);
    log.info("Files moved to {} directory", targetPath);
  }
}
