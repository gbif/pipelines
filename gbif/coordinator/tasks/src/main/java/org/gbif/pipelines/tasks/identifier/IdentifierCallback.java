package org.gbif.pipelines.tasks.identifier;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.tasks.identifier.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.tasks.identifier.validation.PostprocessValidation;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesVerbatimMessage} is received. */
@Slf4j
@AllArgsConstructor
public class IdentifierCallback extends AbstractMessageCallback<PipelinesVerbatimMessage>
    implements StepHandler<PipelinesVerbatimMessage, PipelinesVerbatimMessage> {

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

  @Override
  public boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (!message.getPipelineSteps().contains(StepType.VERBATIM_TO_IDENTIFIER.name())) {
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
          ProcessRunnerBuilder.builder().config(config).message(message).inputPath(path);

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
}
