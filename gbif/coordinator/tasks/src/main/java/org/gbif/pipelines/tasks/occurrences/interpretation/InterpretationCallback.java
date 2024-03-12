package org.gbif.pipelines.tasks.occurrences.interpretation;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.ISO_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY_FORMATS;
import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.GbifApi;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkSettings;
import org.gbif.pipelines.common.process.StackableSparkRunner;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.ingest.java.pipelines.VerbatimToOccurrencePipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
@Builder
public class InterpretationCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesInterpretedMessage> {

  private final InterpreterConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final DatasetClient datasetClient;
  private final CloseableHttpClient httpClient;
  private final ExecutorService executor;

  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);

    PipelinesCallback.<PipelinesVerbatimMessage, PipelinesInterpretedMessage>builder()
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
    PipelinesVerbatimMessage vm = new PipelinesVerbatimMessage();

    String routingKey;
    if (config.validatorOnly) {
      vm.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name()));
      if (config.validatorListenAllMq) {
        routingKey = vm.getRoutingKey() + ".*";
      } else {
        routingKey = vm.setRunner(config.processRunner).getRoutingKey();
      }
    } else {
      routingKey = vm.setRunner(config.processRunner).getRoutingKey();
    }

    log.info("MQ routing key is {}", routingKey);
    return routingKey;
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link InterpreterConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }
    if (isValidator(message.getPipelineSteps(), config.validatorOnly)
        && config.validatorListenAllMq) {
      log.info("Running as a validator task");
      return true;
    }
    boolean isCorrectProcess = config.processRunner.equals(message.getRunner());
    if (!isCorrectProcess) {
      log.info("Skipping, because runner is incorrect");
    }
    return isCorrectProcess;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs
   * verbatim-to-interpreted pipeline
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

      String defaultDateFormat = null;
      if (!isValidator(message.getPipelineSteps(), config.validatorOnly)) {
        defaultDateFormat = getDefaultDateFormat(datasetId);
      }

      Consumer<StringJoiner> beamSettings =
          BeamSettings.occurrenceInterpretation(config, message, path, defaultDateFormat);

      Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

      log.info("Start the process. Message - {}", message);
      try {

        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, beamSettings);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(beamSettings);
        }

        log.info("Deleting old attempts directories");
        String pathToDelete = String.join("/", config.stepConfig.repositoryPath, datasetId);
        HdfsConfigs hdfsConfigs =
            HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
        HdfsUtils.deleteSubFolders(
            hdfsConfigs, pathToDelete, config.deleteAfterDays, Collections.singleton(attempt));

      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesInterpretedMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Long recordsNumber = null;
    Long eventRecordsNumber = null;
    if (message.getValidationResult() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
      eventRecordsNumber = message.getValidationResult().getNumberOfEventRecords();
    }

    boolean repeatAttempt = pathExists(message);
    return new PipelinesInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        recordsNumber,
        eventRecordsNumber,
        null, // Set in balancer cli
        repeatAttempt,
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getValidationResult(),
        message.getInterpretTypes(),
        message.getDatasetType());
  }

  private void runLocal(Consumer<StringJoiner> beamSettings) {
    String[] pipelineOptions = BeamSettings.buildOptions(beamSettings);
    VerbatimToOccurrencePipeline.run(pipelineOptions, executor);
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
            .distributedConfig(config.distributedConfig)
            .kubeConfigFile(config.stackableConfiguration.kubeConfigFile)
            .sparkCrdConfigFile(config.stackableConfiguration.sparkCrdConfigFile)
            .beamConfigFn(beamSettings)
            .sparkAppName(
                getType(message) + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
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

  /** Checks if the directory exists */
  @SneakyThrows
  private boolean pathExists(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String path =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            datasetId,
            attempt,
            DwcTerm.Occurrence.simpleName().toLowerCase());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    return HdfsUtils.exists(hdfsConfigs, path);
  }

  @SneakyThrows
  private String getDefaultDateFormat(String datasetKey) {

    Optional<String> defaultDateFormat =
        GbifApi.getMachineTagValue(
            httpClient, config.stepConfig.registry, datasetKey, "default_date_format");

    if (defaultDateFormat.isEmpty()) {
      return null;
    } else if (defaultDateFormat.get().equals("ISO")) {
      return Arrays.stream(ISO_FORMATS)
          .map(DateComponentOrdering::name)
          .collect(Collectors.joining(","));
    } else if (defaultDateFormat.get().equals("DMY")) {
      return Arrays.stream(DMY_FORMATS)
          .map(DateComponentOrdering::name)
          .collect(Collectors.joining(","));
    } else if (defaultDateFormat.get().equals("MDY")) {
      return Arrays.stream(MDY_FORMATS)
          .map(DateComponentOrdering::name)
          .collect(Collectors.joining(","));
    }
    return null;
  }

  private StepType getType(PipelinesVerbatimMessage message) {
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    return isValidator
        ? StepType.VALIDATOR_VERBATIM_TO_INTERPRETED
        : StepType.VERBATIM_TO_INTERPRETED;
  }
}
