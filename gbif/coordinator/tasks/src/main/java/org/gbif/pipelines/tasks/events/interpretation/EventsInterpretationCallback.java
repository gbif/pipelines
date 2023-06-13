package org.gbif.pipelines.tasks.events.interpretation;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.interpretation.SparkSettings;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.StackableSparkRunner;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
@Builder
public class EventsInterpretationCallback extends AbstractMessageCallback<PipelinesEventsMessage>
    implements StepHandler<PipelinesEventsMessage, PipelinesEventsInterpretedMessage> {

  private static final StepType TYPE = StepType.EVENTS_VERBATIM_TO_INTERPRETED;

  // Required because K8 supports up to 64 characters in names
  private static final String SPARK_NAME_PREFIX = "event-verb-interpreted";

  private final EventsInterpretationConfiguration config;
  private final MessagePublisher publisher;
  private final HdfsConfigs hdfsConfigs;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;

  @Override
  public void handleMessage(PipelinesEventsMessage message) {
    PipelinesCallback.<PipelinesEventsMessage, PipelinesEventsInterpretedMessage>builder()
        .config(config)
        .stepType(TYPE)
        .publisher(publisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  /** Run all the events pipelines in distributed mode */
  @Override
  public String getRouting() {
    return new PipelinesEventsMessage().setRunner("*").getRoutingKey();
  }

  @Override
  public boolean isMessageCorrect(PipelinesEventsMessage message) {
    return message.getDatasetType() == DatasetType.SAMPLING_EVENT
        && message.getNumberOfEventRecords() > 0;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsMessage message) {
    return () -> {
      try {
        log.info("Start the process. Message - {}", message);
        runDistributed(message);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesEventsInterpretedMessage createOutgoingMessage(PipelinesEventsMessage message) {
    boolean repeatAttempt = pathExists(message);
    return new PipelinesEventsInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfOccurrenceRecords(),
        message.getNumberOfEventRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getInterpretTypes(),
        repeatAttempt,
        message.getRunner());
  }

  private void runDistributed(PipelinesEventsMessage message) {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());

    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    String path = String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, verbatim);

    SparkSettings sparkSettings =
        SparkSettings.create(config.sparkConfig, message.getNumberOfEventRecords());

    StackableSparkRunner.StackableSparkRunnerBuilder builder =
        StackableSparkRunner.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .kubeConfigFile(config.stackableConfiguration.kubeConfigFile)
            .sparkCrdConfigFile(config.stackableConfiguration.sparkCrdConfigFile)
            .beamConfigFn(BeamSettings.eventInterpretation(config, message, path))
            .sparkAppName(
                SPARK_NAME_PREFIX + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
            .deleteOnFinish(config.stackableConfiguration.deletePodsOnFinish)
            .sparkSettings(sparkSettings);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /** Checks if the directory exists */
  @SneakyThrows
  private boolean pathExists(PipelinesEventsMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String path =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            datasetId,
            attempt,
            DwcTerm.Event.simpleName().toLowerCase());

    return HdfsUtils.exists(hdfsConfigs, path);
  }
}
