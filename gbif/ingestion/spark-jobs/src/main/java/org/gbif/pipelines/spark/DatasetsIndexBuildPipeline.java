package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.ArgsConstants.*;
import static org.gbif.pipelines.spark.util.FullBuildUtils.checkDatasetTypeSupported;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.spark.util.FullBuildUtils;
import org.gbif.pipelines.spark.util.IndexSettings;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * Re-indexes a list of datasets from their last successful interpretation, to fix some datasets
 * without a {@link FullIndexBuildPipeline}. Each dataset goes through {@link
 * IndexingPipeline#runIndexing} as when it is crawled: its records are loaded into the HBase
 * records table and indexed into the live alias, in its own index or the default one, and its
 * previous documents and indices are removed.
 */
@Slf4j
public class DatasetsIndexBuildPipeline {

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(
        names = "--datasetKeys",
        description = "Comma separated keys of the datasets to index",
        required = true)
    private List<String> datasetKeys = new ArrayList<>();

    @Parameter(names = DATASET_TYPE_ARG, description = "OCCURRENCE or SAMPLING_EVENT")
    private DatasetType datasetType = DatasetType.OCCURRENCE;

    @Parameter(
        names = SOURCE_DIRECTORY_ARG,
        description =
            "Directory containing the parquet to load, by default json for occurrences and "
                + "event_json for events")
    private String sourceDirectory;
  }

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    if (config == null || config.getIndexConfig() == null || config.getElastic() == null) {
      throw new IllegalArgumentException(
          "Invalid configuration file. Missing indexConfig or elastic configuration.");
    }

    checkDatasetTypeSupported(args.datasetType);

    boolean isOccurrence = args.datasetType == DatasetType.OCCURRENCE;
    String sourceDirectory =
        args.sourceDirectory != null
            ? args.sourceDirectory
            : isOccurrence ? Directories.OCCURRENCE_JSON : Directories.EVENT_JSON;
    String schemaPath =
        isOccurrence
            ? config.getIndexConfig().getOccurrenceSchemaPath()
            : config.getIndexConfig().getEventSchemaPath();
    Class<?> recordClass = isOccurrence ? OccurrenceJsonRecord.class : ParentJsonRecord.class;

    SparkSession spark =
        getSparkSession(
            args.master,
            "Index datasets " + args.datasetType,
            config,
            IndexingPipeline::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    List<String> failed = new ArrayList<>();
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      for (String datasetKey : new LinkedHashSet<>(args.datasetKeys)) {
        try {
          if (!index(
              spark,
              fileSystem,
              httpClient,
              config,
              args.datasetType,
              datasetKey.trim(),
              schemaPath,
              recordClass,
              sourceDirectory)) {
            failed.add(datasetKey);
          }
        } catch (Exception e) {
          // carry on with the other datasets
          log.error("Failed to index dataset {}", datasetKey, e);
          failed.add(datasetKey);
        }
      }
    } finally {
      fileSystem.close();
      spark.stop();
      spark.close();
    }

    if (!failed.isEmpty()) {
      throw new IllegalStateException(
          "Failed to index " + failed.size() + " of the datasets: " + String.join(",", failed));
    }
    log.info("Indexed {} datasets", args.datasetKeys.size());
  }

  /** Indexes a dataset, false if it has no successful interpretation */
  private static boolean index(
      SparkSession spark,
      FileSystem fileSystem,
      CloseableHttpClient httpClient,
      PipelinesConfig config,
      DatasetType datasetType,
      String datasetKey,
      String schemaPath,
      Class<?> recordClass,
      String sourceDirectory)
      throws IOException {

    Optional<Integer> attempt =
        FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, datasetKey, sourceDirectory);
    if (attempt.isEmpty()) {
      log.error("Dataset {} has no successful {} to index", datasetKey, sourceDirectory);
      return false;
    }

    String inputPath =
        String.format(
            "%s/%s/%d/%s", config.getOutputPath(), datasetKey, attempt.get(), sourceDirectory);
    long recordCount = spark.read().parquet(inputPath).count();

    // the index of the dataset is chosen as when it's crawled, by its size
    IndexSettings indexSettings =
        IndexSettings.create(
            datasetType,
            config.getIndexConfig(),
            httpClient,
            datasetKey,
            attempt.get(),
            recordCount);

    log.info(
        "Indexing {} records of dataset {} attempt {} into {}",
        recordCount,
        datasetKey,
        attempt.get(),
        indexSettings.getIndexName());

    IndexingPipeline.runIndexing(
        spark,
        fileSystem,
        config,
        datasetKey,
        attempt.get(),
        indexSettings.getIndexAlias(),
        indexSettings.getIndexName(),
        schemaPath,
        indexSettings.getNumberOfShards(),
        recordClass,
        sourceDirectory);
    return true;
  }
}
