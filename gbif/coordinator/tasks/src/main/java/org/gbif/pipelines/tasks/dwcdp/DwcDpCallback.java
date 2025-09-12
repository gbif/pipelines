package org.gbif.pipelines.tasks.dwcdp;

import java.io.File;
import java.nio.file.Paths;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.HttpClient;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.common.process.AirflowSparkLauncher;
import org.gbif.pipelines.common.process.BeamParametersBuilder;
import org.gbif.pipelines.common.process.SparkDynamicSettings;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.utils.file.CompressionUtil;

/** Callback which is called when the {@link DwcDpDownloadFinishedMessage} is received. */
@Slf4j
@Builder
public class DwcDpCallback extends AbstractMessageCallback<DwcDpDownloadFinishedMessage> {

  public static final String DWC_DP_SUFFIX = ".dwcdp";

  private final DwcDpConfiguration config;
  private final MessagePublisher publisher;
  private final HttpClient httpClient;

  @Override
  @SneakyThrows
  public void handleMessage(DwcDpDownloadFinishedMessage message) {
    // Uncompress the dwc-a file
    unCompress(message);

    // Copies all the DP files to HDFS
    copyToHdfs(message);
    String datasetKey = message.getDatasetUuid().toString();
    IndexSettings indexSettings =
        IndexSettings.create(
            config.indexConfig, httpClient, datasetKey, message.getAttempt(), 1_000_000);

    BeamParametersBuilder.BeamParameters beamParameters =
        BeamParametersBuilder.dwcDpIndexing(config, message, indexSettings);

    String dpPath =
        String.valueOf(
            Paths.get(
                config.getRepositoryPath(),
                datasetKey,
                String.valueOf(message.getAttempt()),
                "datapackage.json"));

    beamParameters.addSingleArg(datasetKey, dpPath);

    // Run the Airflow DAG
    runDag(message, beamParameters);
  }

  @SneakyThrows
  private void unCompress(DwcDpDownloadFinishedMessage message) {
    log.info("Uncompressing dwc-a file from message {}", message);
    String datasetKey = message.getDatasetUuid().toString();
    File dwcaFile =
        new File(config.archiveRepository, datasetKey + "/" + datasetKey + DWC_DP_SUFFIX);
    File destinationDir = new File(config.archiveUnpackedRepository, datasetKey);
    if (!destinationDir.exists() && destinationDir.mkdirs()) {
      log.info("Created directory {}", destinationDir.getAbsolutePath());
    }
    CompressionUtil.decompressFile(destinationDir, dwcaFile, true);
    log.info("Finished uncompressing dwc-a file from message {}", message);
  }

  @SneakyThrows
  private void copyToHdfs(DwcDpDownloadFinishedMessage message) {
    log.info("Copying DP files to HDFS from message {}", message);
    String datasetKey = message.getDatasetUuid().toString();
    // Copies all the DP files to HDFS
    Path outputPath =
        HdfsUtils.buildOutputPath(
            config.getRepositoryPath(), datasetKey, String.valueOf(message.getAttempt()));

    FileSystem fs =
        FileSystemFactory.getInstance(
                HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig()))
            .getFs(config.getRepositoryPath());
    Path sourcePath = new Path(config.archiveUnpackedRepository, datasetKey);
    fs.copyFromLocalFile(false, true, sourcePath, outputPath);
    log.info("Finished copying DP files to HDFS from {} to {}", sourcePath, outputPath);
  }

  private void runDag(
      DwcDpDownloadFinishedMessage message, BeamParametersBuilder.BeamParameters beamParameters) {

    // Spark dynamic settings
    boolean x =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkDynamicSettings sparkSettings = SparkDynamicSettings.create(config.sparkConfig, 0L, false);

    // App name
    String sparkAppName = "dwc-dp-" + message.getDatasetUuid() + message.getAttempt();

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfiguration(config.airflowConfig)
        .sparkStaticConfiguration(config.sparkConfig)
        .sparkDynamicSettings(sparkSettings)
        .beamParameters(beamParameters)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();
  }
}
