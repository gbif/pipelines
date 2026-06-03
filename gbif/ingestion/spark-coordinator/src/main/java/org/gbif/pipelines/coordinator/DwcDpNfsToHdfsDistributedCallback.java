package org.gbif.pipelines.coordinator;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpNfsToHdfsMessage;
import org.gbif.pipelines.airflow.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.SparkJobConfig;
import org.gbif.pipelines.util.SparkConfUtil;

@Slf4j
public class DwcDpNfsToHdfsDistributedCallback
    extends AbstractMessageCallback<DwcDpNfsToHdfsMessage>
    implements CloseableMessageCallback<DwcDpNfsToHdfsMessage> {

  private static final String DAG_NAME = "gbif-dwc-dp-nfs-to-hdfs";
  private static final AtomicBoolean running = new AtomicBoolean(true);
  private static final AtomicInteger runningCounter = new AtomicInteger(0);

  private final PipelinesConfig config;

  public DwcDpNfsToHdfsDistributedCallback(PipelinesConfig config, MessagePublisher publisher) {
    this.config = config;
  }

  @Override
  public void handleMessage(DwcDpNfsToHdfsMessage message) {
    runningCounter.incrementAndGet();
    String datasetId = message.getDatasetUuid().toString();
    int attempt = message.getAttempt();
    try {
      log.info("DwcDP distributed nfs-to-hdfs, dataset {}, attempt {}", datasetId, attempt);

      // Build args — DAG handles its own params via merge_params,
      // so we only need the identifiers
      List<String> args = List.of("--datasetId=" + datasetId, "--attempt=" + attempt);

      // Use a dedicated SparkJobConfig from config (no record count needed)
      SparkJobConfig jobConfig = config.getProcessingConfigs().get("");

      String sparkAppName = datasetId + "-" + attempt + "-dwcdp-nfs-to-hdfs";

      SparkConfUtil.Conf conf =
          SparkConfUtil.Conf.builder()
              .description("dwcdp-nfs-to-hdfs")
              .args(args)
              .driverMemoryOverheadFactor(jobConfig.driverMemoryOverheadFactor)
              .driverCores(jobConfig.driverCores)
              .executorMemoryOverheadFactor(jobConfig.executorMemoryOverheadFactor)
              .executorInstances(jobConfig.executorInstances)
              .executorCores(jobConfig.executorCores)
              .defaultParallelism(jobConfig.defaultParallelism)
              .driverMinCpu(jobConfig.driverMinCpu)
              .driverMaxCpu(jobConfig.driverMaxCpu)
              .driverLimitMemory(jobConfig.driverLimitMemory)
              .executorMinCpu(jobConfig.executorMinCpu)
              .executorMaxCpu(jobConfig.executorMaxCpu)
              .executorLimitMemory(jobConfig.executorLimitMemory)
              .build();

      AirflowSparkLauncher.builder()
          .airflowConfig(config.getAirflowConfig())
          .conf(conf)
          .dagName(DAG_NAME)
          .sparkAppName(sparkAppName)
          .build()
          .submitAwaitVoid();

      log.info("DwcDP distributed nfs-to-hdfs complete, dataset {}", datasetId);
    } catch (Exception e) {
      log.error("DwcDP distributed nfs-to-hdfs failed for dataset {}", datasetId, e);
      throw new RuntimeException(e);
    } finally {
      runningCounter.decrementAndGet();
    }
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public int getRunningCounter() {
    return runningCounter.get();
  }

  @Override
  public void init() {}

  @Override
  public void close() {}

  @Override
  public Class<DwcDpNfsToHdfsMessage> getMessageClass() {
    return DwcDpNfsToHdfsMessage.class;
  }
}
