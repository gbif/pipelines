package org.gbif.pipelines.tasks.occurrences.identifier;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.interpretation.SparkSettings;
import org.gbif.pipelines.common.process.ProcessRunnerBeamSettings;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.occurrences.identifier.validation.PostprocessValidation;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
@AllArgsConstructor
public class IdentifierCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesVerbatimMessage> {

  private static final StepType TYPE = StepType.VERBATIM_TO_IDENTIFIER;

  private final IdentifierConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final CloseableHttpClient httpClient;

  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {
    PipelinesCallback.<PipelinesVerbatimMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .config(config)
        .curator(curator)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (!message.getPipelineSteps().contains(TYPE.name())) {
      log.error("The message doesn't contain VERBATIM_TO_IDENTIFIER type");
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
      String datasetId = message.getDatasetUuid().toString();
      String attempt = Integer.toString(message.getAttempt());

      String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
      String path =
          message.getExtraPath() != null
              ? message.getExtraPath()
              : String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, verbatim);

      ProcessRunnerBuilderBuilder builder =
          ProcessRunnerBuilder.builder()
              .distributedConfig(config.distributedConfig)
              .sparkConfig(config.sparkConfig)
              .sparkAppName(
                  TYPE.name() + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
              .beamConfigFn(ProcessRunnerBeamSettings.occurrenceIdentifier(config, message, path));

      log.info("Start the process. Message - {}", message);
      try {

        runDistributed(message, builder);

        PostprocessValidation.builder()
            .httpClient(httpClient)
            .message(message)
            .config(config)
            .build()
            .validate();

      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
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

  private void runDistributed(PipelinesVerbatimMessage message, ProcessRunnerBuilderBuilder builder)
      throws IOException, InterruptedException {

    SparkSettings sparkSettings = SparkSettings.create(config.sparkConfig, config.stepConfig);

    long recordsNumber = sparkSettings.getRecordNumber(message);
    int sparkExecutorNumbers = sparkSettings.computeExecutorNumbers(recordsNumber);

    builder
        .sparkParallelism(sparkSettings.computeParallelism(sparkExecutorNumbers))
        .sparkExecutorMemory(sparkSettings.computeExecutorMemory(sparkExecutorNumbers))
        .sparkExecutorNumbers(sparkExecutorNumbers);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().get().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }
}
