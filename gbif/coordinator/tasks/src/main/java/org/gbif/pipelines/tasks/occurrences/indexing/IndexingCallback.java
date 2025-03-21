package org.gbif.pipelines.tasks.occurrences.indexing;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.util.Collections;
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
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.airflow.AppName;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.common.process.AirflowSparkLauncher;
import org.gbif.pipelines.common.process.BeamParametersBuilder;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkDynamicSettings;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;
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

    log.info("MQ routing key is {}", routingKey);
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

        long interpretationRecordsNumber =
            RecordCountReader.builder()
                .stepConfig(config.stepConfig)
                .datasetKey(message.getDatasetUuid().toString())
                .attempt(message.getAttempt().toString())
                .messageNumber(message.getNumberOfRecords())
                .metaFileName(new InterpreterConfiguration().metaFileName)
                .metricName(Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED)
                .alternativeMetricName(Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED)
                .skipIf(true)
                .build()
                .get();

        long dwcaRecordsNumber =
            RecordCountReader.builder()
                .stepConfig(config.stepConfig)
                .datasetKey(message.getDatasetUuid().toString())
                .attempt(message.getAttempt().toString())
                .metaFileName(new DwcaToAvroConfiguration().metaFileName)
                .metricName(Metrics.ARCHIVE_TO_OCC_COUNT)
                .alternativeMetricName(Metrics.ARCHIVE_TO_ER_COUNT)
                .skipIf(true)
                .build()
                .get();

        if (interpretationRecordsNumber == 0 && dwcaRecordsNumber == 0) {
          throw new PipelinesException(
              "No data to index. Both interpretationRecordsNumber and dwcaRecordsNumber have 0 records, check ingestion metadata yaml files");
        }

        long recordsNumber = Math.min(dwcaRecordsNumber, interpretationRecordsNumber);
        if (interpretationRecordsNumber == 0) {
          recordsNumber = dwcaRecordsNumber;
        } else if (dwcaRecordsNumber == 0) {
          recordsNumber = interpretationRecordsNumber;
        }

        IndexSettings indexSettings =
            IndexSettings.create(
                config.indexConfig,
                httpClient,
                message.getDatasetUuid().toString(),
                message.getAttempt(),
                recordsNumber);
        BeamParameters beamParameters =
            BeamParametersBuilder.occurrenceIndexing(config, message, indexSettings);

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, beamParameters, recordsNumber);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(beamParameters);
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

  private void runLocal(BeamParameters beamParameters) {
    InterpretedToEsIndexExtendedPipeline.run(beamParameters.toArray(), executor);
  }

  private void runDistributed(
      PipelinesInterpretedMessage message, BeamParameters beamParameters, long recordsNumber) {

    // Spark dynamic settings
    boolean useMemoryExtraCoef =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(config.sparkConfig, recordsNumber, useMemoryExtraCoef);

    // App name
    String sparkAppName =
        AppName.get(getType(message), message.getDatasetUuid(), message.getAttempt());

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

  private StepType getType(PipelinesInterpretedMessage message) {
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    return isValidator ? StepType.VALIDATOR_INTERPRETED_TO_INDEX : StepType.INTERPRETED_TO_INDEX;
  }
}
