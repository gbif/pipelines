package org.gbif.pipelines.tasks.occurrences.identifier;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
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
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkSettings;
import org.gbif.pipelines.common.process.StackableSparkRunner;
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

        Consumer<StringJoiner> beamSettings =
            BeamSettings.occurrenceIdentifier(config, message, getFilePath(message));

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, beamSettings);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(beamSettings);
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

  private void runDistributed(PipelinesVerbatimMessage message, Consumer<StringJoiner> beamSettings)
      throws IOException {

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
            .build()
            .get();

    boolean useMemoryExtraCoef =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkSettings sparkSettings =
        SparkSettings.create(config.sparkConfig, recordsNumber, useMemoryExtraCoef);

    StackableSparkRunner.StackableSparkRunnerBuilder builder =
        StackableSparkRunner.builder()
            .beamConfigFn(beamSettings)
            .kubeConfigFile(config.stackableConfiguration.kubeConfigFile)
            .sparkCrdConfigFile(config.stackableConfiguration.sparkCrdConfigFile)
            .sparkConfiguration(config.sparkConfig)
            .sparkAppName(TYPE.name() + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
            .distributedConfig(config.distributedConfig)
            .deleteOnFinish(config.stackableConfiguration.deletePodsOnFinish)
            .sparkSettings(sparkSettings);

    // Assembles a terminal java process and runs it
    StackableSparkRunner ssr = builder.build();
    int exitValue = ssr.start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException(
          "Process failed in distributed Job. Check k8s logs " + ssr.getSparkAppName());
    } else {
      log.info("Process has been finished, Spark job name - {}", ssr.getSparkAppName());
    }
  }

  private void runLocal(Consumer<StringJoiner> beamSettings) {
    String[] pipelineOptions = BeamSettings.buildOptions(beamSettings);
    VerbatimToIdentifierPipeline.run(pipelineOptions, executor);
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
