package org.gbif.pipelines.tasks.identifier;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.ISO_FORMATS;
import static org.gbif.common.parsers.date.DateComponentOrdering.MDY_FORMATS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MachineTag;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.tasks.identifier.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
@AllArgsConstructor
public class IdentifierCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesVerbatimMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
        .stepType(StepType.VERBATIM_TO_IDENTIFIER)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link IdentifierConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }
    boolean isCorrectProcess = config.processRunner.equals(message.getRunner());
    if (!isCorrectProcess) {
      log.info("Skipping, because runner is incorrect");
    }
    return isCorrectProcess;
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

      String defaultDateFormat = getDefaultDateFormat(datasetId);

      ProcessRunnerBuilderBuilder builder =
          ProcessRunnerBuilder.builder()
              .config(config)
              .message(message)
              .inputPath(path)
              .defaultDateFormat(defaultDateFormat);

      log.info("Start the process. Message - {}", message);
      try {

        runDistributed(message, builder);

        runPostprocessValidation(message);

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
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Set<String> pipelineSteps = new HashSet<>(message.getPipelineSteps());
    pipelineSteps.remove(StepType.VERBATIM_TO_IDENTIFIER.name());

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
        message.getExecutionId());
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

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(hdfsConfigs, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);

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

  private void runPostprocessValidation(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = config.metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    ToDoubleFunction<String> getMetricFn =
        m -> {
          try {
            HdfsConfigs hdfsConfigs =
                HdfsConfigs.create(
                    config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
            return HdfsUtils.getDoubleByKey(hdfsConfigs, metaPath, m + Metrics.ATTEMPTED)
                .orElse(0d);
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    double invalidIdCount = getMetricFn.applyAsDouble(Metrics.INVALID_GBIF_ID_COUNT);
    double duplicateIdCount = getMetricFn.applyAsDouble(Metrics.DUPLICATE_GBIF_IDS_COUNT);
    double uniqieIdCount = getMetricFn.applyAsDouble(Metrics.UNIQUE_GBIF_IDS_COUNT);

    if (uniqieIdCount == 0d) {
      log.error(
          "Interpreted records {}, invalid records {}, duplicate  records {}",
          uniqieIdCount,
          invalidIdCount,
          duplicateIdCount);
      throw new IllegalArgumentIOException("No records with valid GBIF ID!");
    }

    if (invalidIdCount != 0d || duplicateIdCount != 0d) {
      double duplicatePercent =
          (invalidIdCount + duplicateIdCount)
              * 100
              / (invalidIdCount + duplicateIdCount + uniqieIdCount);

      if (duplicatePercent > config.failIfDuplicateIdPercent) {
        log.error(
            "GBIF IDs hit maximum allowed threshold: allowed - {}%, duplicates - {}%",
            config.failIfDuplicateIdPercent, duplicatePercent);
        throw new IllegalArgumentIOException("GBIF IDs hit maximum allowed threshold");
      } else {
        log.warn(
            "GBIF IDs current duplicates rate: allowed - {}%, duplicates - {}%",
            config.failIfDuplicateIdPercent, duplicatePercent);
      }
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
            Interpretation.DIRECTORY_NAME);

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    return HdfsUtils.exists(hdfsConfigs, path);
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
