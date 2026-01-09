package org.gbif.pipelines.core.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.api.model.pipelines.InterpretationType.RecordType.ALL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.CRAP_USER;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.USER_GROUP;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

/** Utility class to work with file system. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FsUtils {

  public static final String HDFS_EMR_PREFIX = "hdfs:///";

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
  public static FileSystem createParentDirectories(HdfsConfigs hdfsConfigs, Path path) {
    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(path.toString());
    fs.mkdirs(path.getParent());
    return fs;
  }

  /** Removes temporal directory */
  public static void removeTmpDirectory(String path) {
    File tmp = Paths.get(path).toFile();
    if (tmp.exists()) {
      try {
        FileUtils.deleteDirectory(tmp);
        log.info("temp directory {} deleted", tmp.getPath());
      } catch (IOException e) {
        log.error("Could not delete temp directory {}", tmp.getPath(), e);
      }
    }
  }

  /** Removes temporal directory, before closing Main thread */
  public static void removeTmpDirectoryAfterShutdown(String path) {
    Runnable runnable = () -> removeTmpDirectory(path);
    Runtime.getRuntime().addShutdownHook(new Thread(runnable));
  }

  /** Helper method to get file system based on provided configuration. */
  @SneakyThrows
  public static FileSystem getFileSystem(HdfsConfigs hdfsConfigs, String path) {
    return FileSystemFactory.getInstance(hdfsConfigs).getFs(path);
  }

  /** Helper method to get file system based on provided configuration. */
  @SneakyThrows
  public static FileSystem getLocalFileSystem(HdfsConfigs hdfsConfigs) {
    return FileSystemFactory.getInstance(hdfsConfigs).getLocalFs();
  }

  /** Helper method to write/overwrite a file */
  public static void createFile(FileSystem fs, String path, String body) throws IOException {
    path = convertLocalHdfsPath(path);
    try (FSDataOutputStream stream = fs.create(new Path(path), true)) {
      stream.writeBytes(body);
    }
  }

  /**
   * Deletes all directories and subdirectories(recursively) by file prefix name.
   *
   * <p>Example: all directories with '.temp-' prefix in directory
   * '89aad0bb-654f-483c-8711-2c00551033ae/3'
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param directoryPath to a directory
   * @param filePrefix file name prefix
   */
  public static void deleteDirectoryByPrefix(
      HdfsConfigs hdfsConfigs, String directoryPath, String filePrefix) {
    FileSystem fs = getFileSystem(hdfsConfigs, directoryPath);
    try {
      deleteDirectoryByPrefix(fs, new Path(directoryPath), filePrefix);
    } catch (IOException e) {
      log.warn("Can't delete folder - {}, prefix - {}", directoryPath, filePrefix);
    }
  }

  /**
   * Use crap as an owner for directories and subdirectories(recursively).
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param path to a directory/file
   */
  public static void setOwnerToCrap(HdfsConfigs hdfsConfigs, String path) {
    setOwner(hdfsConfigs, path, CRAP_USER, USER_GROUP);
  }

  /**
   * Set owner for directories and subdirectories(recursively).
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param path to a directory/file
   * @param userName – e.g. "crap"
   * @param groupName – e.g. "supergroup"
   */
  public static void setOwner(
      HdfsConfigs hdfsConfigs, String path, String userName, String groupName) {
    FileSystem fs = getFileSystem(hdfsConfigs, path);
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
      log.warn("Can't change permissions for folder/file - {}", path, e);
    }
  }

  /**
   * Moves a list files that match against a glob filter into a target directory.
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param globFilter filter used to filter files and paths
   * @param targetPath target directory
   */
  public static void moveDirectory(HdfsConfigs hdfsConfigs, String targetPath, String globFilter) {
    FileSystem fs = getFileSystem(hdfsConfigs, targetPath);
    try {
      FileStatus[] status = fs.globStatus(new Path(globFilter));
      Path[] paths = FileUtil.stat2Paths(status);
      for (Path path : paths) {
        boolean rename = fs.rename(path, new Path(targetPath, path.getName()));
        log.info("File {} moved status - {}", path, rename);
      }
    } catch (IOException e) {
      log.warn("Can't move files using filter - {}, into path - {}", globFilter, targetPath, e);
    }
  }

  /**
   * Deletes a list files that match against a glob filter into a target directory.
   *
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param globFilter filter used to filter files and paths
   */
  public static void deleteByPattern(
      HdfsConfigs hdfsConfigs, String directoryPath, String globFilter) {
    FileSystem fs = getFileSystem(hdfsConfigs, directoryPath);
    try {
      FileStatus[] status = fs.globStatus(new Path(globFilter));
      Path[] paths = FileUtil.stat2Paths(status);
      for (Path path : paths) {
        fs.delete(path, Boolean.TRUE);
      }
    } catch (IOException e) {
      log.warn("Can't delete files using filter - {}", globFilter, e);
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
   * @param hdfsConfigs path to hdfs-site.xml config file
   * @param directoryPath path to some directory
   */
  public static boolean deleteIfExist(HdfsConfigs hdfsConfigs, String directoryPath) {
    FileSystem fs = getFileSystem(hdfsConfigs, directoryPath);
    directoryPath = convertLocalHdfsPath(directoryPath);

    Path path = new Path(directoryPath);
    try {
      return fs.exists(path) && fs.delete(path, true);
    } catch (IOException e) {
      log.error("Can't delete {} directory, cause - {}", directoryPath, e.getCause());
      return false;
    }
  }

  /**
   * Convert EMR style path with hdfs:/// prefix to local path.
   *
   * <p>eg. hdfs:///mypath/123 to /mypath/123
   *
   * <p>new Path(hdfs:///mypath/123) will be interpreted as hdfs:/mypath/123 which will cause a
   * wrong FS exception.
   */
  public static String convertLocalHdfsPath(String directoryPath) {
    if (directoryPath.startsWith(HDFS_EMR_PREFIX)) {
      // convert EMR style path hdfs:///mypath/123 to /mypath/123
      directoryPath = directoryPath.substring(7);
    }
    return directoryPath;
  }

  /**
   * Read a properties file from HDFS/Local FS
   *
   * @param hdfsConfigs HDFS config file
   * @param filePath properties file path
   */
  @SneakyThrows
  public static <T> T readConfigFile(HdfsConfigs hdfsConfigs, String filePath, Class<T> clazz) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, filePath);
    Path fPath = new Path(filePath);
    if (fs.exists(fPath)) {
      log.info("Reading properties path - {}", filePath);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath), UTF_8))) {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

        SimpleModule keyTermDeserializer = new SimpleModule();
        keyTermDeserializer.addKeyDeserializer(
            Term.class,
            new KeyDeserializer() {
              @Override
              public Term deserializeKey(String value, DeserializationContext dc) {
                return TermFactory.instance().findTerm(value);
              }
            });
        mapper.registerModule(keyTermDeserializer);

        mapper.findAndRegisterModules();
        return mapper.readValue(br, clazz);
      }
    }
    throw new FileNotFoundException("The properties file doesn't exist - " + filePath);
  }

  /** Deletes directories if a dataset with the same attempt was interpreted before */
  public static void deleteInterpretIfExist(
      HdfsConfigs hdfsConfigs,
      String basePath,
      String datasetId,
      Integer attempt,
      DwcTerm coreTerm,
      Set<String> steps) {
    if (steps != null && !steps.isEmpty()) {

      String path =
          String.join(
              "/", basePath, datasetId, attempt.toString(), coreTerm.simpleName().toLowerCase());

      if (steps.contains(ALL.name())) {
        log.info("Delete interpretation directory - {}", path);
        boolean isDeleted = deleteIfExist(hdfsConfigs, path);
        log.info("Delete interpretation directory - {}, deleted - {}", path, isDeleted);
      } else {
        for (String step : steps) {
          log.info("Delete {}/{} directory", path, step.toLowerCase());
          boolean isDeleted =
              deleteIfExist(hdfsConfigs, String.join("/", path, step.toLowerCase()));
          log.info("Delete interpretation directory - {}, deleted - {}", path, isDeleted);
        }
      }
    }
  }

  public static void deleteInterpretIfExist(
      HdfsConfigs hdfsConfigs,
      String basePath,
      String datasetId,
      Integer attempt,
      DwcTerm coreTerm,
      String... steps) {
    Set<String> s = new HashSet<>(Arrays.asList(steps));
    deleteInterpretIfExist(hdfsConfigs, basePath, datasetId, attempt, coreTerm, s);
  }

  /**
   * Check if the file exists
   *
   * @param hdfsConfigs HDFS config file
   * @param filePath path to the file
   */
  @SneakyThrows
  public static boolean fileExists(HdfsConfigs hdfsConfigs, String filePath) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, filePath);
    Path fPath = new Path(filePath);
    return fs.exists(fPath);
  }

  public static <T> boolean deleteAvroFileIfEmpty(
      HdfsConfigs hdfsConfigs, String path, Class<T> avroClass) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    Path fPath = new Path(path);
    return deleteAvroFileIfEmpty(fs, fPath, avroClass);
  }

  @SneakyThrows
  public static <T> boolean deleteAvroFileIfEmpty(
      FileSystem fs, Path mainPath, Class<T> avroClass) {
    if (!fs.exists(mainPath)) {
      return true;
    }

    Predicate<Path> deleteFn =
        path -> {
          try {
            boolean hasNoRecords;
            SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(avroClass);
            try (AvroFSInput input =
                    new AvroFSInput(fs.open(path), fs.getFileStatus(path).getLen());
                DataFileReader<T> dataFileReader = new DataFileReader<>(input, datumReader)) {
              hasNoRecords = !dataFileReader.hasNext();
            }
            if (hasNoRecords) {
              log.warn("File is empty - {}", path);
              fs.delete(path, true);
              return true;
            }
            return false;
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    if (fs.isFile(mainPath)) {
      return deleteFn.test(mainPath);
    } else {
      boolean result = false;
      for (Path p : getFilesByExt(fs, mainPath, AVRO_EXTENSION)) {

        boolean r = deleteFn.test(p);
        if (r) {
          result = r;
        }
      }
      return result;
    }
  }

  @SneakyThrows
  public static List<Path> getFilesByExt(FileSystem fs, Path path, String filterExt) {
    List<Path> paths = new ArrayList<>();
    if (fs.exists(path)) {
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);
      while (files.hasNext()) {
        LocatedFileStatus next = files.next();
        Path np = next.getPath();
        if (next.isFile() && np.getName().endsWith(filterExt)) {
          paths.add(np);
        }
      }
    }
    return paths;
  }

  public static void createFile(FileSystem fs, Path path, String body) throws IOException {
    try (FSDataOutputStream stream = fs.create(path, true)) {
      stream.writeChars(body);
    }
  }
}
