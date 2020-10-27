package au.org.ala.utils;

import au.org.ala.kvs.ALAPipelinesConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.ingest.options.BasePipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.utils.FileSystemFactory;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;

/** Extensions to FSUtils. See {@link FsUtils} */
@Slf4j
public class ALAFsUtils {

  /**
   * Constructs the path for reading / writing identifiers. This is written outside of /interpreted
   * directory.
   *
   * <p>Example /data/pipelines-data/dr893/1/identifiers/ala_uuid where name = 'ala_uuid'
   *
   * @param options
   * @param name
   * @param uniqueId
   * @return
   */
  public static String buildPathIdentifiersUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "identifiers", false),
            name,
            "interpret-" + uniqueId)
        .toString();
  }

  public static String buildPathMultimediaUsingTargetPath(BasePipelineOptions options) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "interpreted", false), "multimedia")
        .toString();
  }

  public static String buildPathMultimediaDeltaUsingTargetPath(BasePipelineOptions options) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "multimedia-delta", false), "multimedia-delta")
        .toString();
  }

  /**
   * Constructs the path for reading / writing generalised data. This is written outside of
   * /interpreted directory.
   *
   * <p>Example /data/pipelines-data/dr893/1/generalised/ala_sensitive_taxon where name =
   * 'ala_sensitive_taxon'
   *
   * @param options
   * @param name
   * @param uniqueId
   * @return
   */
  public static String buildPathGeneralisedUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "generalised", false),
            name,
            "generalise-" + uniqueId)
        .toString();
  }

  /**
   * Constructs the path for reading / writing sampling. This is written outside of /interpreted
   * directory.
   *
   * <p>Example /data/pipelines-data/dr893/1/sampling/ala_uuid where name = 'ala_uuid'
   *
   * @param options
   * @param name
   * @param uniqueId
   * @return
   */
  public static String buildPathSamplingUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "sampling", false),
            name,
            name + "-" + uniqueId)
        .toString();
  }

  public static String buildPathImageServiceUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "images", false), name + "-" + uniqueId)
        .toString();
  }

  /**
   * Build a path to sampling output.
   *
   * @param options
   * @return
   */
  public static String buildPathSamplingOutputUsingTargetPath(
      InterpretationPipelineOptions options) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "sampling", false),
            PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION_FEATURE
                .toString()
                .toLowerCase(),
            PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION_FEATURE
                .toString()
                .toLowerCase())
        .toString();
  }

  /**
   * Build a path to sampling downloads.
   *
   * @param options
   * @return
   */
  public static String buildPathSamplingDownloadsUsingTargetPath(
      InterpretationPipelineOptions options) {
    return FsUtils.buildPath(
            FsUtils.buildDatasetAttemptPath(options, "sampling", false), "downloads")
        .toString();
  }

  /**
   * Removes a directory with content if the folder exists
   *
   * @param directoryPath path to some directory
   */
  public static boolean deleteIfExist(FileSystem fs, String directoryPath) {
    Path path = new Path(directoryPath);
    try {
      return fs.exists(path) && fs.delete(path, true);
    } catch (IOException e) {
      log.error("Can't delete {} directory, cause - {}", directoryPath, e.getCause());
      return false;
    }
  }

  /** Helper method to write/overwrite a file */
  public static WritableByteChannel createByteChannel(FileSystem fs, String path)
      throws IOException {
    FSDataOutputStream stream = fs.create(new Path(path), true);
    return Channels.newChannel(stream);
  }

  /** Helper method to write/overwrite a file */
  public static OutputStream openOutputStream(FileSystem fs, String path) throws IOException {
    return fs.create(new Path(path), true);
  }

  /** Helper method to write/overwrite a file */
  public static ReadableByteChannel openByteChannel(FileSystem fs, String path) throws IOException {
    FSDataInputStream stream = fs.open(new Path(path));
    return Channels.newChannel(stream);
  }

  /** Helper method to write/overwrite a file */
  public static InputStream openInputStream(FileSystem fs, String path) throws IOException {
    return fs.open(new Path(path));
  }

  /**
   * Returns true if the supplied path exists.
   *
   * @param fs
   * @param directoryPath
   * @return
   * @throws IOException
   */
  public static boolean exists(FileSystem fs, String directoryPath) throws IOException {
    Path path = new Path(directoryPath);
    return fs.exists(path);
  }

  /**
   * Returns true if the supplied path exists.
   *
   * @param fs
   * @param directoryPath
   * @return
   * @throws IOException
   */
  public static boolean createDirectory(FileSystem fs, String directoryPath) throws IOException {
    Path path = new Path(directoryPath);
    return fs.mkdirs(new Path(directoryPath));
  }

  /**
   * Retrieve a list of files in the supplied path.
   *
   * @param fs
   * @param directoryPath
   * @return
   * @throws IOException
   */
  public static Collection<String> listPaths(FileSystem fs, String directoryPath)
      throws IOException {

    Path path = new Path(directoryPath);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
    List<String> filePaths = new ArrayList<String>();
    while (iterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path filePath = locatedFileStatus.getPath();
      filePaths.add(filePath.toString());
    }
    return filePaths;
  }

  public static void deleteMetricsFile(InterpretationPipelineOptions options) {
    String metadataPath =
        FsUtils.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), metadataPath);
    deleteIfExist(fs, metadataPath);
  }

  /**
   * Read a properties file from HDFS/Local FS
   *
   * @param hdfsSiteConfig HDFS config file
   * @param filePath properties file path
   */
  @SneakyThrows
  public static ALAPipelinesConfig readConfigFile(
      String hdfsSiteConfig, String coreSiteConfig, String filePath) {
    FileSystem fs = FsUtils.getLocalFileSystem(hdfsSiteConfig, coreSiteConfig);
    Path fPath = new Path(filePath);
    if (fs.exists(fPath)) {
      log.info("Reading properties path - {}", filePath);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath)))) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        mapper.findAndRegisterModules();
        ALAPipelinesConfig config = mapper.readValue(br, ALAPipelinesConfig.class);
        if (config.getGbifConfig() == null) {
          config.setGbifConfig(new PipelinesConfig());
        }
        return config;
      }
    }
    throw new FileNotFoundException("The properties file doesn't exist - " + filePath);
  }

  public static boolean checkAndCreateLockFile(InterpretationPipelineOptions options)
      throws IOException {
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getInputPath());

    Path path = new Path(options.getInputPath() + ".lockdir");
    if (fs.exists(path)) {
      // dataset is locked
      log.info("lockdir exists: " + options.getInputPath() + ".lockdir");
      return false;
    }

    log.info("Creating lockdir: " + options.getInputPath() + ".lockdir");
    // otherwise, lock it and return true
    try {
      return fs.mkdirs(new Path(options.getInputPath() + ".lockdir"));
    } catch (IOException e) {
      log.info("Unable to create lockdir");
      return false;
    }
  }

  public static void deleteLockFile(InterpretationPipelineOptions options) throws IOException {

    String lockFilePath = options.getInputPath() + ".lockdir";

    log.info("Attempting to delete lock file {}", lockFilePath);
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), lockFilePath);
  }

  /**
   * Scans the supplied options.getInputPath() for zip files. Assumes zip files are in the name for
   * of <DATASET_ID>.zip
   *
   * @param options
   * @return a Map of datasetId -> filePath, with zip files sorted by size, largest to smallest.
   * @throws IOException
   */
  public static Map<String, String> listAllDatasets(
      String hdfsSiteConfig, String coreSiteConfig, String inputPath) throws IOException {

    FileSystem fs = FileSystemFactory.getInstance(hdfsSiteConfig, coreSiteConfig).getFs(inputPath);

    log.info("List files in inputPath: {}", inputPath);

    Path path = new Path(inputPath);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);

    Map<Path, Long> filePathsWithSize = new HashMap<>();

    // find zip files
    while (iterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path filePath = locatedFileStatus.getPath();

      long fileLength = locatedFileStatus.getLen();
      if (filePath.getName().endsWith(".zip")) {
        log.debug(filePath.getName() + " : " + fileLength);
        filePathsWithSize.put(filePath, fileLength);
      }
    }

    // sort by size and return ordered map
    return filePathsWithSize.entrySet().stream()
        .sorted(Map.Entry.<Path, Long>comparingByValue().reversed())
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().getName().replaceAll(".zip", ""),
                entry -> entry.getKey().getParent() + "/" + entry.getKey().getName(),
                (e1, e2) -> e1,
                LinkedHashMap::new));
  }
}
