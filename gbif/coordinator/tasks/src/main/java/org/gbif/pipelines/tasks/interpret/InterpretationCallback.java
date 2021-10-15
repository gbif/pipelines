package org.gbif.pipelines.tasks.interpret;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.ISO_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY_FORMATS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.tasks.interpret.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
public class InterpretationCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesInterpretedMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final InterpreterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final CloseableHttpClient httpClient;
  private final ExecutorService executor;

  public InterpretationCallback(
      InterpreterConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ValidationWsClient validationClient,
      CloseableHttpClient httpClient,
      ExecutorService executor) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.historyClient = historyClient;
    this.validationClient = validationClient;
    this.httpClient = httpClient;
    this.executor = executor;
  }

  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {
    StepType type =
        message.isValidator() || config.validatorOnly
            ? StepType.VALIDATOR_VERBATIM_TO_INTERPRETED
            : StepType.VERBATIM_TO_INTERPRETED;

    PipelinesCallback.<PipelinesVerbatimMessage, PipelinesInterpretedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .curator(curator)
        .stepType(type)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
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
    if ((message.isValidator() || config.validatorOnly) && config.validatorListenAllMq) {
      return true;
    }
    return config.processRunner.equals(message.getRunner());
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
      if (!config.validatorOnly && !message.isValidator()) {
        defaultDateFormat = getDefaultDateFormat(datasetId);
      }

      ProcessRunnerBuilderBuilder builder =
          ProcessRunnerBuilder.builder()
              .config(config)
              .message(message)
              .inputPath(path)
              .defaultDateFormat(defaultDateFormat);

      Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

      log.info("Start the process. Message - {}", message);
      try {
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(builder);
        }

        log.info("Deleting old attempts directories");
        String pathToDelete = String.join("/", config.stepConfig.repositoryPath, datasetId);
        HdfsUtils.deleteSubFolders(
            config.stepConfig.hdfsSiteConfig,
            config.stepConfig.coreSiteConfig,
            pathToDelete,
            config.deleteAfterDays);

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
    if (message.getValidationResult() != null
        && message.getValidationResult().getNumberOfRecords() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
    }

    boolean repeatAttempt = pathExists(message);

    return new PipelinesInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        recordsNumber,
        null, // Set in balancer cli
        repeatAttempt,
        message.getResetPrefix(),
        null,
        null,
        message.getEndpointType(),
        message.getValidationResult(),
        message.getInterpretTypes(),
        message.isValidator() || config.validatorOnly);
  }

  private void runLocal(ProcessRunnerBuilderBuilder builder) {
    VerbatimToInterpretedPipeline.run(builder.build().buildOptions(), executor);
  }

  private void runDistributed(PipelinesVerbatimMessage message, ProcessRunnerBuilderBuilder builder)
      throws IOException, InterruptedException {
    long recordsNumber = getRecordNumber(message);
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);

    builder
        .sparkParallelism(computeSparkParallelism(sparkExecutorNumbers))
        .sparkExecutorMemory(computeSparkExecutorMemory(sparkExecutorNumbers))
        .sparkExecutorNumbers(sparkExecutorNumbers);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().get().start().waitFor();

    if (exitValue != 0) {
      throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
    } else {
      log.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(int executorNumbers) {
    int count = executorNumbers * config.sparkConfig.executorCores * 2;

    if (count < config.sparkConfig.parallelismMin) {
      return config.sparkConfig.parallelismMin;
    }
    if (count > config.sparkConfig.parallelismMax) {
      return config.sparkConfig.parallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and max is
   * config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {

    if (sparkExecutorNumbers < config.sparkConfig.executorMemoryGbMin) {
      return config.sparkConfig.executorMemoryGbMin + "G";
    }
    if (sparkExecutorNumbers > config.sparkConfig.executorMemoryGbMax) {
      return config.sparkConfig.executorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkConfig.executorNumbersMin and max
   * is config.sparkConfig.executorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers =
        (int)
            Math.ceil(
                (double) recordsNumber
                    / (config.sparkConfig.executorCores * config.sparkConfig.recordsPerThread));
    if (sparkExecutorNumbers < config.sparkConfig.executorNumbersMin) {
      return config.sparkConfig.executorNumbersMin;
    }
    if (sparkExecutorNumbers > config.sparkConfig.executorNumbersMax) {
      return config.sparkConfig.executorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /** Reads number of records from the message or archive-to-avro metadata file */
  private long getRecordNumber(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    Long messageNumber =
        message.getValidationResult() != null
                && message.getValidationResult().getNumberOfRecords() != null
            ? message.getValidationResult().getNumberOfRecords()
            : null;
    String fileNumber =
        HdfsUtils.getValueByKey(
            config.stepConfig.hdfsSiteConfig,
            config.stepConfig.coreSiteConfig,
            metaPath,
            Metrics.ARCHIVE_TO_ER_COUNT);

    if (messageNumber == null && (fileNumber == null || fileNumber.isEmpty())) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (messageNumber == null) {
      return Long.parseLong(fileNumber);
    }

    if (fileNumber == null || fileNumber.isEmpty()) {
      return messageNumber;
    }

    return messageNumber > Long.parseLong(fileNumber) ? messageNumber : Long.parseLong(fileNumber);
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
            Interpretation.DIRECTORY_NAME);

    return HdfsUtils.exists(
        config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig, path);
  }

  @SneakyThrows
  private String getDefaultDateFormat(String datasetKey) {
    String url = config.stepConfig.registry.wsUrl + "/dataset/" + datasetKey + "/machineTag";
    HttpResponse response = httpClient.execute(new HttpGet(url));
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }

    List<MachineTag> machineTags =
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<MachineTag>>() {});

    Optional<String> defaultDateFormat =
        machineTags.stream()
            .filter(x -> x.getName().equals("default_date_format"))
            .map(MachineTag::getValue)
            .findFirst();

    if (!defaultDateFormat.isPresent()) {
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
}
