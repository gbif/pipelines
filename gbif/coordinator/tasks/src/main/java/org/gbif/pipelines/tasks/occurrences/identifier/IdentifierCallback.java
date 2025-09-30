package org.gbif.pipelines.tasks.occurrences.identifier;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.airflow.AppName;
import org.gbif.pipelines.common.hdfs.HdfsViewSettings;
import org.gbif.pipelines.common.process.AirflowSparkLauncher;
import org.gbif.pipelines.common.process.BeamParametersBuilder;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkDynamicSettings;
import org.gbif.pipelines.ingest.java.pipelines.VerbatimToIdentifierPipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.occurrences.identifier.validation.IdentifierValidationResult;
import org.gbif.pipelines.tasks.occurrences.identifier.validation.PostprocessValidation;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
@Builder
public class IdentifierCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesVerbatimMessage> {

  private static final StepType TYPE = StepType.VERBATIM_TO_IDENTIFIER;

  private final IdentifierConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final CloseableHttpClient httpClient;
  private final ExecutorService executor;

  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {
    PipelinesCallback.<PipelinesVerbatimMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .config(config)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return new PipelinesVerbatimMessage()
        .setPipelineSteps(Collections.singleton(StepType.VERBATIM_TO_IDENTIFIER.name()))
        .setRunner(config.processRunner)
        .getRoutingKey();
  }

  @Override
  public boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (!message.getPipelineSteps().contains(TYPE.name())) {
      log.error("The message doesn't contain {} type", TYPE);
      return false;
    }
    return true;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs
   * verbatim-to-identifier beam pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesVerbatimMessage message) {
    return () -> {
      log.info("Start the process. Message - {}", message);
      try {

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        int numberOfShards = computeNumberOfShards(message);

        BeamParameters beamParameters =
            BeamParametersBuilder.occurrenceIdentifier(
                config, message, getFilePath(message), numberOfShards);

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, beamParameters);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(beamParameters);
        }

        IdentifierValidationResult validationResult =
            PostprocessValidation.builder()
                .httpClient(httpClient)
                .message(message)
                .config(config)
                .build()
                .validate();

        if (validationResult.isResultValid()) {
          log.info(validationResult.getValidationMessage());
        } else {
          historyClient.notifyAbsentIdentifiers(
              message.getDatasetUuid(),
              message.getAttempt(),
              message.getExecutionId(),
              validationResult.getValidationMessage());
          log.error(validationResult.getValidationMessage());
          if (config.cleanAndMarkAsAborted) {
            historyClient.markPipelineStatusAsAborted(message.getExecutionId());
          }
          throw new PipelinesException(validationResult.getValidationMessage());
        }

      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new PipelinesException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  private int computeNumberOfShards(PipelinesVerbatimMessage message) {
    Long numberOfRecords = message.getValidationResult().getNumberOfRecords();
    return HdfsViewSettings.computeNumberOfShards(config.avroConfig, numberOfRecords);
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Set<String> pipelineSteps = new HashSet<>(message.getPipelineSteps());
    pipelineSteps.remove(TYPE.name());

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getInterpretTypes(),
        pipelineSteps,
        message.getRunner(),
        message.getEndpointType(),
        message.getExtraPath(),
        message.getValidationResult(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getDatasetType());
  }

  private void runDistributed(PipelinesVerbatimMessage message, BeamParameters beamParameters)
      throws IOException {

    // Spark dynamic settings
    Long messageNumber =
        message.getValidationResult() != null
                && message.getValidationResult().getNumberOfRecords() != null
            ? message.getValidationResult().getNumberOfRecords()
            : null;

    long recordsNumber =
        RecordCountReader.builder()
            .stepConfig(config.stepConfig)
            .datasetKey(message.getDatasetUuid().toString())
            .attempt(message.getAttempt().toString())
            .messageNumber(messageNumber)
            .metaFileName(new DwcaToAvroConfiguration().metaFileName)
            .metricName(Metrics.ARCHIVE_TO_OCC_COUNT)
            .alternativeMetricName(Metrics.ARCHIVE_TO_ER_COUNT)
            .build()
            .get();

    boolean useMemoryExtraCoef =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(config.sparkConfig, recordsNumber, useMemoryExtraCoef);

    // App name
    String sparkAppName = AppName.get(TYPE, message.getDatasetUuid(), message.getAttempt());

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

  private void runLocal(BeamParameters beamParameters) {
    VerbatimToIdentifierPipeline.run(beamParameters.toArray(), executor);
  }

  private String getFilePath(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());

    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    return message.getExtraPath() != null
        ? message.getExtraPath()
        : String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, verbatim);
  }
}
