package org.gbif.pipelines.tasks.occurrences.indexing;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.common.indexing.SparkSettings;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.ProcessRunnerBuilder;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
@Slf4j
@Builder
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesIndexedMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final HttpClient httpClient;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final DatasetClient datasetClient;
  private final ExecutorService executor;

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesIndexedMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(getType(message))
        .isValidator(isValidator)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    PipelinesInterpretedMessage pm = new PipelinesInterpretedMessage();

    String routingKey;
    if (config.validatorOnly) {
      pm.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_INTERPRETED_TO_INDEX.name()));
      if (config.validatorListenAllMq) {
        routingKey = pm.getRoutingKey() + ".*";
      } else {
        routingKey = pm.setRunner(config.processRunner).getRoutingKey();
      }
    } else {
      routingKey = pm.setRunner(config.processRunner).getRoutingKey();
    }

    log.info("MQ rounting key is {}", routingKey);
    return routingKey;
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link IndexingConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    if (isValidator && config.validatorListenAllMq) {
      log.info("Running as a validator task");
      return true;
    }
    StepType type = getType(message);
    if (!message.getPipelineSteps().contains(type.name())) {
      log.info("Skipping, because expected step is {}", type);
      return false;
    }
    boolean isCorrectProcess = config.processRunner.equals(message.getRunner());
    if (!isCorrectProcess) {
      log.info("Skipping, because runner is incorrect");
    }
    return isCorrectProcess;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
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

        ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .distributedConfig(config.distributedConfig)
                .sparkConfig(config.sparkConfig)
                .sparkAppName(
                    getType(message) + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
                .beamConfigFn(BeamSettings.occurreceIndexing(config, message, indexSettings));

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder, recordsNumber);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(builder);
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        null, // Set in balancer cli
        null,
        message.getEndpointType());
  }

  private void runLocal(ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder) {
    InterpretedToEsIndexExtendedPipeline.run(builder.build().buildOptions(), executor);
  }

  private void runDistributed(
      PipelinesInterpretedMessage message,
      ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder,
      long recordsNumber)
      throws IOException, InterruptedException {

    String filePath =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            message.getDatasetUuid().toString(),
            Integer.toString(message.getAttempt()),
            DwcTerm.Occurrence.simpleName().toLowerCase(),
            RecordType.BASIC.name().toLowerCase());

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
   * Reads number of records from an archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfRecords();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            hdfsConfigs, metaPath, Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED);
    if (!fileNumber.isPresent()) {
      fileNumber =
          HdfsUtils.getLongByKey(
              hdfsConfigs, metaPath, Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED);
    }

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

  private StepType getType(PipelinesInterpretedMessage message) {
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    return isValidator ? StepType.VALIDATOR_INTERPRETED_TO_INDEX : StepType.INTERPRETED_TO_INDEX;
  }
}
