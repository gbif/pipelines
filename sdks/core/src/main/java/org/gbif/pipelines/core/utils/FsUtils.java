package org.gbif.pipelines.core.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;

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
import java.util.function.Consumer;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.pipelines.core.factory.FileSystemFactory;

/** Utility class to work with file system. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FsUtils {

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

  /**
   * Helper method to create a parent directory in the provided path
   *
   * @return filesystem
   */
  @SneakyThrows
  public static FileSystem createParentDirectories(
      String hdfsSiteConfig, String coreSiteConfig, Path path) {
    FileSystem fs =
        FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(path.toString());
    fs.mkdirs(path.getParent());
    return fs;
  }

  /** Removes temporal directory, before closing Main thread */
  public static void removeTmpDirectory(String path) {
    Runnable runnable =
        () -> {
          File tmp = Paths.get(path).toFile();
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
   * Set owner for directories and subdirectories(recursively).
   *
   * @param hdfsSiteConfig path to hdfs-site.xml config file
   * @param path to a directory/file
   * @param userName – e.g. "crap"
   * @param groupName – e.g. "supergroup"
   */
  public static void setOwner(
      String hdfsSiteConfig,
      String coreSiteConfig,
      String path,
      String userName,
      String groupName) {
    FileSystem fs = getFileSystem(hdfsSiteConfig, coreSiteConfig, path);
    try {

      Consumer<Path> fn =
          p -> {
            try {
              fs.setOwner(p, userName, groupName);
            } catch (IOException e) {
              log.warn("Can't change owner for folder/file - {}", path);
            }
          };

      Path p = new Path(path);
      if (fs.isDirectory(p)) {
        // Files
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(p, true);
        while (iterator.hasNext()) {
          LocatedFileStatus fileStatus = iterator.next();
          fn.accept(fileStatus.getPath());
        }
        // Directories
        FileStatus[] fileStatuses = fs.listStatus(p);
        for (FileStatus fst : fileStatuses) {
          fn.accept(fst.getPath());
        }
      }
      fn.accept(p);

    } catch (IOException e) {
      log.warn("Can't change permissions for folder/file - {}", path);
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
        log.info("File {} moved status - {}", path, rename);
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
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath), UTF_8))) {
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
   * Check if the file exists
   *
   * @param hdfsSiteConfig HDFS config file
   * @param filePath path to the file
   */
  @SneakyThrows
  public static boolean fileExists(String hdfsSiteConfig, String coreSiteConfig, String filePath) {
    FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, coreSiteConfig, filePath);
    Path fPath = new Path(filePath);
    return fs.exists(fPath);
  }
}
