package org.gbif.pipelines.coordinator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.util.TableUtil;

@Slf4j
public class DatasetDeleteCallback extends AbstractMessageCallback<DeleteDatasetOccurrencesMessage>
    implements CloseableMessageCallback<DeleteDatasetOccurrencesMessage> {

  private final PipelinesConfig pipelinesConfig;
  private final SparkSession sparkSession;
  private final FileSystem fileSystem;
  private final DatasetType datasetType;
  public static final String SHUTDOWN_FILE_PATH = "/tmp/shutdown_now";
  private static final AtomicInteger runningCounter = new AtomicInteger(0);

  public DatasetDeleteCallback(
      PipelinesConfig pipelinesConfig, String sparkMaster, DatasetType datasetType) {
    super();
    this.pipelinesConfig = pipelinesConfig;
    this.datasetType = datasetType;

    try {
      SparkSession.Builder sparkBuilder = SparkSession.builder().appName("pipelines_standalone");
      sparkBuilder = sparkBuilder.master(sparkMaster);
      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder
          .enableHiveSupport()
          .config("spark.hadoop.hive.metastore.uris", this.pipelinesConfig.getHiveMetastoreUris());

      this.sparkSession = sparkBuilder.getOrCreate();
      Configuration hadoopConf = this.sparkSession.sparkContext().hadoopConfiguration();
      if (pipelinesConfig.getHdfsSiteConfig() != null
          && pipelinesConfig.getCoreSiteConfig() != null) {
        hadoopConf.addResource(new Path(pipelinesConfig.getHdfsSiteConfig()));
        hadoopConf.addResource(new Path(pipelinesConfig.getCoreSiteConfig()));
        this.fileSystem = FileSystem.get(hadoopConf);
      } else {
        log.warn("Using local filesystem - this is suitable for local development only");
        this.fileSystem = FileSystem.getLocal(hadoopConf);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void handleMessage(DeleteDatasetOccurrencesMessage message) {

    runningCounter.incrementAndGet();
    try {
      log.info("Deleting dataset {}....", message.getDatasetUuid());

      String esAlias =
          datasetType == DatasetType.OCCURRENCE
              ? pipelinesConfig.getIndexConfig().getOccurrenceAlias()
              : pipelinesConfig.getIndexConfig().getEventAlias();

      // remove from iceberg tables
      log.info("Deleting dataset {} from iceberg", message.getDatasetUuid());
      TableUtil.deleteRecordsForDataset(
          this.sparkSession,
          this.pipelinesConfig,
          message.getDatasetUuid().toString(),
          datasetType);

      // remove from elastic
      log.info(
          "Deleting dataset {} from elastic indexes for {}", message.getDatasetUuid(), datasetType);
      EsConfig esConfig = EsConfig.from(pipelinesConfig.getIndexConfig().getDefaultIndexCatUrl());

      // remove the dataset from Es indexes
      EsIndex.deleteRecordsByDatasetId(
          esConfig,
          new String[] {esAlias},
          message.getDatasetUuid().toString(),
          idxName -> true,
          60,
          5);

      // remove files to avoid inclusion in index  & table rebuilds
      deleteFileSystemOutputs(message);

      log.info("Deleted dataset {}", message.getDatasetUuid());
    } finally {
      runningCounter.decrementAndGet();
    }
  }

  private void deleteFileSystemOutputs(DeleteDatasetOccurrencesMessage message) {
    try {
      log.info("Deleting dataset outputs from file system for {}", message.getDatasetUuid());
      // remove hdfs & json directories for this dataset for all attempts
      String[] targets =
          datasetType == DatasetType.OCCURRENCE
              ? new String[] {Directories.OCCURRENCE_HDFS, Directories.OCCURRENCE_JSON}
              : new String[] {Directories.EVENT_HDFS, Directories.EVENT_JSON};

      for (String target : targets) {
        Path pattern =
            new Path(
                pipelinesConfig.getOutputPath() + "/" + message.getDatasetUuid() + "/*/" + target);
        FileStatus[] matches = fileSystem.globStatus(pattern);
        if (matches != null) {
          for (FileStatus status : matches) {
            log.info("Deleting dataset outputs from file system for {}", status.getPath());
            fileSystem.delete(status.getPath(), true);
          }
        }
      }

    } catch (IOException e) {
      log.warn("Failed to delete dataset {} from hdfs", message.getDatasetUuid(), e);
    }
  }

  @Override
  public boolean isRunning() {
    return !new File(SHUTDOWN_FILE_PATH).exists();
  }

  @Override
  public int getRunningCounter() {
    return runningCounter.get();
  }

  @Override
  public void init() throws IOException {}

  @Override
  public void close() throws IOException {
    log.info("Closing Spark Session and FileSystem connections");
    if (sparkSession != null) {
      sparkSession.close();
    }
    if (fileSystem != null) {
      fileSystem.close();
    }
    log.info("Closed Spark Session and FileSystem connections");
  }
}
