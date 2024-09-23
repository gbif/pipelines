package org.gbif.pipelines.tasks.occurrences.warehouse;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.airflow.AppName;
import org.gbif.pipelines.common.messaging.DataWarehouseMessage;
import org.gbif.pipelines.common.process.AirflowSparkLauncher;
import org.gbif.pipelines.common.process.BeamParametersBuilder;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkDynamicSettings;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.tasks.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when an instance {@link PipelinesInterpretationMessage} is received. */
@Slf4j
@Builder
public class DataWarehouseCallback extends AbstractMessageCallback<PipelinesHdfsViewMessage>
    implements StepHandler<PipelinesHdfsViewMessage, DataWarehouseMessage> {

  protected final HdfsViewConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final CommonHdfsViewCallback commonHdfsViewCallback;

  @Override
  public void handleMessage(PipelinesHdfsViewMessage message) {
    PipelinesCallback.<PipelinesHdfsViewMessage, DataWarehouseMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .config(config)
        .stepType(config.stepType)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return new PipelinesHdfsViewMessage().setRunner(config.processRunner).getRoutingKey();
  }

  /** Main message processing logic, creates a terminal java process, which runs */
  @Override
  public Runnable createRunnable(PipelinesHdfsViewMessage message) {
    return () -> {
      BeamParametersBuilder.BeamParameters beamParameters =
          BeamParametersBuilder.occurrenceWarehouse(config, message);
      runDistributed(message, beamParameters);
    };
  }

  @SneakyThrows
  private void runDistributed(
      PipelinesHdfsViewMessage message, BeamParametersBuilder.BeamParameters beamParameters) {

    long recordsNumber =
        RecordCountReader.builder()
            .stepConfig(config.stepConfig)
            .datasetKey(message.getDatasetUuid().toString())
            .attempt(message.getAttempt().toString())
            .metaFileName(new DwcaToAvroConfiguration().metaFileName)
            .metricName(PipelinesVariables.Metrics.ARCHIVE_TO_OCC_COUNT)
            .build()
            .get();

    log.info("Calculate job's settings based on {} records", recordsNumber);
    boolean useMemoryExtraCoef =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkDynamicSettings sparkDynamicSettings =
        SparkDynamicSettings.create(config.sparkConfig, recordsNumber, useMemoryExtraCoef);

    // App name
    String sparkAppName =
        AppName.get(config.stepType, message.getDatasetUuid(), message.getAttempt());

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfiguration(config.airflowConfig)
        .sparkStaticConfiguration(config.sparkConfig)
        .sparkDynamicSettings(sparkDynamicSettings)
        .beamParameters(beamParameters)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();
  }

  @Override
  public DataWarehouseMessage createOutgoingMessage(PipelinesHdfsViewMessage message) {
    return new DataWarehouseMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps(), null, null);
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesHdfsViewMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }

    if (!config.processRunner.equals(message.getRunner())) {
      log.warn("Skipping, because runner is incorrect");
      return false;
    }
    return true;
  }
}
