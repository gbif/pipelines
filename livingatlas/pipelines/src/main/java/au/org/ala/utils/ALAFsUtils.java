package au.org.ala.utils;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.BasePipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;

/** Extensions to FSUtils. See {@link FsUtils} */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAFsUtils {

  /**
   * Constructs the path for reading / writing identifiers. This is written outside of /interpreted
   * directory.
   *
   * <p>Example /data/pipelines-data/dr893/1/identifiers/ala_uuid where name = 'ala_uuid'
   */
  public static String buildPathIdentifiersUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return PathBuilder.buildPath(
            PathBuilder.buildDatasetAttemptPath(options, "identifiers", false),
            name,
            "interpret-" + uniqueId)
        .toString();
  }

  public static String buildPathMultimediaUsingTargetPath(BasePipelineOptions options) {
    return PathBuilder.buildPath(
            PathBuilder.buildDatasetAttemptPath(options, "interpreted", false), "multimedia")
        .toString();
  }

  public static String buildPathImageServiceUsingTargetPath(
      BasePipelineOptions options, String name, String uniqueId) {
    return PathBuilder.buildPath(
            PathBuilder.buildDatasetAttemptPath(options, "images", false), name + "-" + uniqueId)
        .toString();
  }

  /** Build a path to sampling downloads. */
  public static String buildPathSamplingUsingTargetPath(AllDatasetsPipelinesOptions options) {
    if (options.getDatasetId() == null
        || "all".equals(options.getDatasetId())
        || "*".equals(options.getDatasetId())) {
      return String.join("/", options.getAllDatasetsInputPath(), "sampling");
    }
    return PathBuilder.buildDatasetAttemptPath(options, "sampling", false);
  }

  /**
   * Build a path to outlier records. {fsPath}/pipelines-outlier/{datasetId}
   * {fsPath}/pipelines-outlier/all
   */
  public static String buildPathOutlierUsingTargetPath(AllDatasetsPipelinesOptions options)
      throws IOException {
    // default: {fsPath}/pipelines-outlier
    FileSystem fs =
        FileSystemFactory.getInstance(options.getHdfsSiteConfig(), options.getCoreSiteConfig())
            .getFs(options.getTargetPath());

    String outputPath = PathBuilder.buildPath(options.getTargetPath()).toString();

    // {fsPath}/pipelines-outlier/{datasetId}
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      outputPath = PathBuilder.buildPath(outputPath, options.getDatasetId()).toString();
    } else {
      // {fsPath}/pipelines-outlier/all
      outputPath = PathBuilder.buildPath(outputPath, "all").toString();
    }
    // delete previous runs
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), outputPath);
    ALAFsUtils.createDirectory(fs, outputPath);
    return outputPath;
  }

  /**
   * Removes a directory with content if the folder exists
   *
   * @param directoryPath path to some directory
   */
  public static boolean existsAndNonEmpty(FileSystem fs, String directoryPath) {
    Path path = new Path(directoryPath);
    try {
      boolean exists = fs.exists(path);
      log.info(" {} exists - {}", path, exists);
      if (!exists) {
        return false;
      }
      if (log.isDebugEnabled()) {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, true);
        while (iter.hasNext()) {
          log.debug("File: {}", iter.next().getPath().toString());
        }
      }
      return fs.listFiles(path, true).hasNext();
    } catch (IOException e) {
      log.error("Can't delete {} directory, cause - {}", directoryPath, e.getCause());
      return false;
    }
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

  /** Returns true if the supplied path exists. */
  public static boolean exists(FileSystem fs, String directoryPath) throws IOException {
    Path path = new Path(directoryPath);
    return fs.exists(path);
  }

  /** Returns true if the supplied path exists. */
  public static boolean createDirectory(FileSystem fs, String directoryPath) throws IOException {
    return fs.mkdirs(new Path(directoryPath));
  }

  /** Retrieve a list of files in the supplied path. */
  public static Collection<String> listPaths(FileSystem fs, String directoryPath)
      throws IOException {

    Path path = new Path(directoryPath);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
    List<String> filePaths = new ArrayList<>();
    while (iterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path filePath = locatedFileStatus.getPath();
      filePaths.add(filePath.toString());
    }
    return filePaths;
  }

  public static void deleteMetricsFile(InterpretationPipelineOptions options) {
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
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

  public static void deleteLockFile(InterpretationPipelineOptions options) {

    String lockFilePath = options.getInputPath() + ".lockdir";

    log.info("Attempting to delete lock file {}", lockFilePath);
    FsUtils.deleteIfExist(options.getHdfsSiteConfig(), options.getCoreSiteConfig(), lockFilePath);
  }

  /**
   * Scans the supplied options.getInputPath() for zip files. Assumes zip files are in the name for
   * of <DATASET_ID>.zip
   *
   * @return a Map of datasetId -> filePath, with zip files sorted by size, largest to smallest.
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

  /**
   * Load index records from AVRO.
   *
   * @param options
   * @param p
   * @return
   */
  public static PCollection<IndexRecord> loadIndexRecords(
      AllDatasetsPipelinesOptions options, Pipeline p) {

    String dataResourceFolder = options.getDatasetId();
    if (dataResourceFolder == null || "all".equalsIgnoreCase(dataResourceFolder)) {
      dataResourceFolder = "*";
    }
    String dataSource =
        String.join(
            "/", options.getAllDatasetsInputPath(), "index-record", dataResourceFolder, "*.avro");
    log.info("Loading index records from: " + dataSource);
    return p.apply(AvroIO.read(IndexRecord.class).from(dataSource));
  }
}
