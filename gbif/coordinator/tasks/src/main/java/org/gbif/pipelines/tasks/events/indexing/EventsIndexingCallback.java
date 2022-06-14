package org.gbif.pipelines.tasks.events.indexing;

import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.client.HttpClient;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.common.indexing.SparkSettings;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
public class EventsIndexingCallback
    extends AbstractMessageCallback<PipelinesEventsInterpretedMessage>
    implements StepHandler<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage> {

  private static final StepType TYPE = StepType.EVENTS_INTERPRETED_TO_INDEX;

  private final EventsIndexingConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final HdfsConfigs hdfsConfigs;
  private final PipelinesHistoryClient historyClient;

  public EventsIndexingCallback(
      EventsIndexingConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      HttpClient httpClient,
      PipelinesHistoryClient historyClient) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    this.httpClient = httpClient;
    this.historyClient = historyClient;
  }

  @Override
  public void handleMessage(PipelinesEventsInterpretedMessage message) {
    PipelinesCallback.<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage>builder()
        .config(config)
        .curator(curator)
        .stepType(TYPE)
        .publisher(publisher)
        .historyClient(historyClient)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesEventsInterpretedMessage message) {
    return message.getNumberOfEventRecords() > 0;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsInterpretedMessage message) {
    return () -> {
      try {
        long recordsNumber = getRecordNumber(message);

        IndexSettings indexSettings =
            IndexSettings.create(
                config.indexConfig,
                httpClient,
                message.getDatasetUuid().toString(),
                message.getAttempt(),
                recordsNumber);

        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .distributedConfig(config.distributedConfig)
                .sparkConfig(config.sparkConfig)
                .sparkAppName(TYPE + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
                .beamConfigFn(BeamSettings.eventIndexing(config, message, indexSettings));

        log.info("Start the process. Message - {}", message);
        runDistributed(message, builder, recordsNumber);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesEventsIndexedMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getRunner());
  }

  private void runDistributed(
      PipelinesEventsInterpretedMessage message,
      ProcessRunnerBuilderBuilder builder,
      long recordsNumber)
      throws IOException, InterruptedException {

    String filePath =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            message.getDatasetUuid().toString(),
            Integer.toString(message.getAttempt()),
            DwcTerm.Event.simpleName().toLowerCase(),
            RecordType.EVENT.name().toLowerCase());

    SparkSettings sparkSettings =
        SparkSettings.create(config.sparkConfig, config.stepConfig, filePath, recordsNumber);

    builder.sparkSettings(sparkSettings);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().get().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesEventsInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new EventsInterpretationConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfEventRecords();
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(hdfsConfigs, metaPath, Metrics.UNIQUE_IDS_COUNT + Metrics.ATTEMPTED);

    if (messageNumber == null && !fileNumber.isPresent()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (messageNumber == null) {
      return fileNumber.get();
    }

    if (!fileNumber.isPresent() || messageNumber > fileNumber.get()) {
      return messageNumber;
    }
    return fileNumber.get();
  }
}
