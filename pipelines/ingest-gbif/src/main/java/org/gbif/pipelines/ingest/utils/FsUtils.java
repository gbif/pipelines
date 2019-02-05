package org.gbif.pipelines.ingest.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.parsers.exception.IORuntimeException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;

/** Utility class to work with file system. */
public final class FsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FsUtils.class);

  private FsUtils() {}

  /** Build a {@link Path} from an array of string values using path separator. */
  public static Path buildPath(String... values) {
    StringJoiner joiner = new StringJoiner(Path.SEPARATOR);
    Arrays.stream(values).forEach(joiner::add);
    return new Path(joiner.toString());
  }

  /** Build a {@link String} path from an array of string values using path separator. */
  public static String buildPathString(String... values) {
    return buildPath(values).toString();
  }

  /**
   * Uses pattern for path - "{targetPath}/{datasetId}/{attempt}/{name}"
   *
   * @return string path
   */
  public static String buildPath(BasePipelineOptions options, String name) {
    return FsUtils.buildPath(
        options.getTargetPath(),
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
  public static String buildPathInterpret(
      BasePipelineOptions options, String name, String uniqueId) {

    return FsUtils.buildPath(
        options.getTargetPath(),
        options.getDatasetId(),
        options.getAttempt().toString(),
        Interpretation.DIRECTORY_NAME,
        name.toLowerCase(),
        Interpretation.FILE_NAME + uniqueId)
        .toString();
  }

  /**
   * Gets temporary directory from options or returns default value.
   *
   * @return path to a temporary directory.
   */
  public static String getTempDir(BasePipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPathString(options.getTargetPath(), "tmp")
        : options.getTempLocation();
  }

  /**
   * Reads Beam options from arguments or file.
   *
   * @return array of Beam arguments.
   */
  public static String[] readArgsAsFile(String[] args) {
    if (args != null && args.length == 1) {
      String file = args[0];
      if (file.endsWith(".properties")) {
        try {
          return Files.readAllLines(Paths.get(file))
              .stream()
              .filter(x -> !Strings.isNullOrEmpty(x))
              .map(x -> x.startsWith("--") ? x : "--" + x)
              .toArray(String[]::new);
        } catch (IOException ex) {
          throw new IORuntimeException(ex);
        }
      }
    }
    return args;
  }

  /** Removes temporal directory, before closing Main thread */
  public static void removeTmpDirectory(BasePipelineOptions options) {
    Runnable runnable =
        () -> {
          File tmp = Paths.get(getTempDir(options)).toFile();
          if (tmp.exists()) {
            try {
              FileUtils.deleteDirectory(tmp);
              LOG.info("temp directory {} deleted", tmp.getPath());
            } catch (IOException e) {
              LOG.error("Could not delete temp directory {}", tmp.getPath());
            }
          }
        };

    Runtime.getRuntime().addShutdownHook(new Thread(runnable));
  }

  /**
   * Helper method to get file system based on provided configuration.
   */
  public static FileSystem getFileSystem(String hdfsSiteConfig, String path) {
    try {
      Configuration config = new Configuration();

      // check if the hdfs-site.xml is provided
      if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
        File hdfsSite = new File(hdfsSiteConfig);
        if (hdfsSite.exists() && hdfsSite.isFile()) {
          LOG.info("using hdfs-site.xml");
          config.addResource(hdfsSite.toURI().toURL());
        } else {
          LOG.warn("hdfs-site.xml does not exist");
        }
      }

      return FileSystem.get(URI.create(path), config);
    } catch (IOException ex) {
      throw new IllegalStateException("Can't get a valid filesystem from provided uri " + path, ex);
    }
  }

  /**
   * Helper method to write/overwrite a file
   */
  public static void createFile(FileSystem fs, String path, String body) throws IOException {
    try (FSDataOutputStream stream = fs.create(new Path(path), true)) {
      stream.writeChars(body);
    }
  }

  /**
   * Deletes all directories and subdirectories(recursively) by file prefix name.
   * <p>
   * Example: all directories with '.temp-' prefix in direcory '89aad0bb-654f-483c-8711-2c00551033ae/3'
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param directoryPath to a directory
   * @param filePrefix file name prefix
   */
  public static void deleteDirectoryByPrefix(String hdfsSiteConfig, String directoryPath, String filePrefix) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, directoryPath);
    try {
      deleteDirectoryByPrefix(fs, new Path(directoryPath), filePrefix);
    } catch (IOException e) {
      LOG.warn("Can't delete folder - {}, prefix - {}", directoryPath, filePrefix);
    }
  }

  private static void deleteDirectoryByPrefix(FileSystem fs, Path directoryPath, String filePrefix) throws IOException {
    FileStatus[] status = fs.listStatus(directoryPath);
    List<Path> list = Arrays.stream(status)
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
  public static boolean deleteIfExist(String hdfsSiteConfig, String directoryPath) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, directoryPath);

    Path path = new Path(directoryPath);
    try {
      return fs.exists(path) && fs.delete(path, true);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Deletes directories if a dataset with the same attempt was interpreted before
   */
  public static void deleteInterpretIfExist(String hdfsSiteConfig, String datasetId, String attempt, List<String> steps) {
    if (steps != null && !steps.isEmpty()) {

      String path = String.join("/", hdfsSiteConfig, datasetId, attempt, Interpretation.DIRECTORY_NAME);

      if (steps.contains(ALL.name())) {
        deleteIfExist(hdfsSiteConfig, path);
      } else {
        for (String step : steps) {
          deleteIfExist(hdfsSiteConfig, String.join("/", path, step.toLowerCase()));
        }
      }
    }
  }
}
