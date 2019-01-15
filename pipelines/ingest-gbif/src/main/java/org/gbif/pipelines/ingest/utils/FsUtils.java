package org.gbif.pipelines.ingest.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.StringJoiner;

import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.parsers.exception.IORuntimeException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

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
        "interpreted",
        name.toLowerCase(),
        "interpret-" + uniqueId)
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
  public static void removeTmpDirecrory(BasePipelineOptions options) {
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
   * TODO:DOC
   */
  public static void createFile(FileSystem fs, String path, String body) throws IOException {
    try (FSDataOutputStream stream = fs.create(new Path(path), false)) {
      stream.writeChars(body);
    }
  }
}
