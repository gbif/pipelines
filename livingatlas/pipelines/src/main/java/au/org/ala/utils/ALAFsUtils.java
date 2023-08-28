package au.org.ala.utils;

import static org.gbif.pipelines.core.utils.FsUtils.convertLocalHdfsPath;

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
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.BasePipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.IndexRecord;

/** Extensions to FSUtils. See {@link FsUtils} */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAFsUtils {

  /**
   * Creates a path object, handling the case of an EMR style path.
   *
   * @param path
   * @return
   */
  public static Path createPath(String path) {
    return new Path(convertLocalHdfsPath(path));
  }

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
            PathBuilder.buildDatasetAttemptPath(
                options, DwcTerm.Occurrence.simpleName().toLowerCase(), false),
            "multimedia")
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
   * NOTE: It will delete the existing folder Build a path to outlier records.
   * {fsPath}/pipelines-outlier/{datasetId} {fsPath}/pipelines-outlier/all
   */
  public static String buildPathOutlierUsingTargetPath(
      AllDatasetsPipelinesOptions options, boolean delete) throws IOException {

    // default: {fsPath}/pipelines-outlier
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(options.getTargetPath());

    String outputPath = options.getTargetPath();

    // {fsPath}/pipelines-outlier/{datasetId}
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      outputPath = outputPath + "/" + options.getDatasetId();
    } else {
      // {fsPath}/pipelines-outlier/all
      outputPath = outputPath + "/" + "all";
    }
    // delete previous runs
    if (delete) FsUtils.deleteIfExist(hdfsConfigs, outputPath);
    else {
      if (!exists(fs, outputPath)) ALAFsUtils.createDirectory(fs, outputPath);
    }

    return outputPath;
  }

  /**
   * Get an output path to outlier records. {fsPath}/pipelines-outlier/{datasetId}
   * {fsPath}/pipelines-outlier/all
   */
  public static String getOutlierTargetPath(AllDatasetsPipelinesOptions options) {

    String outputPath = options.getTargetPath();

    // {fsPath}/pipelines-outlier/{datasetId}
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      outputPath = PathBuilder.buildPath(outputPath, options.getDatasetId(), "*.avro").toString();
    } else {
      // {fsPath}/pipelines-outlier/all
      outputPath = PathBuilder.buildPath(outputPath, "all", "*.avro").toString();
    }

    return outputPath;
  }

  /**
   * Check if a directory exists and is not empty
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
    Path path = createPath(directoryPath);
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
    FSDataOutputStream stream = fs.create(createPath(path), true);
    return Channels.newChannel(stream);
  }

  /** Helper method to write/overwrite a file */
  public static OutputStream openOutputStream(FileSystem fs, String path) throws IOException {
    return fs.create(createPath(path), true);
  }

  /** Helper method to write/overwrite a file */
  public static ReadableByteChannel openByteChannel(FileSystem fs, String path) throws IOException {
    FSDataInputStream stream = fs.open(createPath(path));
    return Channels.newChannel(stream);
  }

  /** Helper method to write/overwrite a file */
  public static InputStream openInputStream(FileSystem fs, String path) throws IOException {
    return fs.open(createPath(path));
  }

  /** Returns true if the supplied path exists. */
  public static boolean exists(FileSystem fs, String directoryPath) throws IOException {
    Path path = createPath(directoryPath);
    log.info("Using filesystem {} for path {}", fs.toString(), directoryPath);
    return fs.exists(path);
  }

  public static boolean hasFiles(FileSystem fs, String wildcardPath) throws IOException {
    Path path = createPath(wildcardPath);
    return fs.globStatus(path).length > 0;
  }

  /** Returns true if the supplied path exists. */
  public static boolean createDirectory(FileSystem fs, String directoryPath) throws IOException {
    return fs.mkdirs(createPath(directoryPath));
  }

  /** Retrieve a list of files in the supplied path. */
  public static Collection<String> listPaths(FileSystem fs, String directoryPath)
      throws IOException {

    Path path = createPath(directoryPath);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
    List<String> filePaths = new ArrayList<>();
    while (iterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path filePath = locatedFileStatus.getPath();
      filePaths.add(filePath.toString());
    }
    return filePaths;
  }

  /**
   * Read a properties file from HDFS/Local FS
   *
   * @param hdfsConfigs HDFS config file
   * @param filePath properties file path
   */
  @SneakyThrows
  public static ALAPipelinesConfig readConfigFile(HdfsConfigs hdfsConfigs, String filePath) {
    FileSystem fs = FsUtils.getLocalFileSystem(hdfsConfigs);
    log.info("Reading from filesystem - {}", fs);
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

  /**
   * Scans the supplied options.getInputPath() for zip files. Assumes zip files are in the name for
   * of <DATASET_ID>.zip
   *
   * @return a Map of datasetId -> filePath, with zip files sorted by size, largest to smallest.
   */
  public static Map<String, String> listAllDatasets(HdfsConfigs hdfsConfigs, String inputPath)
      throws IOException {

    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(inputPath);

    log.info("List files in inputPath: {}", inputPath);
    Path path = createPath(inputPath);
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
                entry -> entry.getKey().toString(),
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
