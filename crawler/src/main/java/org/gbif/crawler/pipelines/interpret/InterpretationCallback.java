package org.gbif.crawler.pipelines.interpret;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.base.Strings;

import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPathAsString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesVerbatimMessage} is received.
 * <p>
 * The main method is {@link InterpretationCallback#handleMessage}
 */
public class InterpretationCallback extends AbstractMessageCallback<PipelinesVerbatimMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationCallback.class);
  private static final StepType STEP = StepType.VERBATIM_TO_INTERPRETED;

  private final InterpreterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private PipelinesHistoryWsClient historyWsClient;
  private final ExecutorService executor;

  InterpretationCallback(InterpreterConfiguration config, MessagePublisher publisher, CuratorFramework curator,
                         PipelinesHistoryWsClient historyWsClient, ExecutorService executor) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
    this.historyWsClient = historyWsClient;
    this.executor = executor;
  }

  /**
   * Handles a MQ {@link PipelinesVerbatimMessage} message
   */
  @Override
  public void handleMessage(PipelinesVerbatimMessage message) {

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = Optional.ofNullable(message.getAttempt()).orElseGet(() -> getLatestAttempt(message));

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      if (!isMessageCorrect(message)) {
        LOG.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      LOG.info("Message handler began - {}", message);

      // Common variables
      Set<String> steps = message.getPipelineSteps();
      Runnable runnable = createRunnable(message);

      Long recordsNumber = null;
      if (message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null) {
        recordsNumber = message.getValidationResult().getNumberOfRecords();
      }

      boolean repeatAttempt = pathExists(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesInterpretedMessage(datasetId, attempt, steps, recordsNumber, repeatAttempt, message.getResetPrefix()))
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyWsClient(historyWsClient)
          .metricsSupplier(metricsSupplier(datasetId, attempt))
          .build()
          .handleMessage();

      LOG.info("Message handler ended - {}", message);

    }
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link InterpreterConfiguration#processRunner}
   */
  private boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs verbatim-to-interpreted pipeline
   */
  private Runnable createRunnable(PipelinesVerbatimMessage message) {
    return () -> {
      String datasetId = message.getDatasetUuid().toString();
      String attempt = Integer.toString(message.getAttempt());

      String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
      String path = message.getExtraPath() != null ? message.getExtraPath() :
          String.join("/", config.repositoryPath, datasetId, attempt, verbatim);

      ProcessRunnerBuilder builder = ProcessRunnerBuilder.create()
          .config(config)
          .message(message)
          .inputPath(path);

      Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

      LOG.info("Start the process. Message - {}", message);
      try {
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(builder);
        }

        LOG.info("Deleting old attempts directories");
        String pathToDelete = String.join("/", config.repositoryPath, datasetId);
        HdfsUtils.deleteSubFolders(config.hdfsSiteConfig, pathToDelete, config.deleteAfterDays);

      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  private void runLocal(ProcessRunnerBuilder builder) throws Exception {
    if (config.standaloneUseJava) {
      VerbatimToInterpretedPipeline.run(builder.buildOptions(), executor);
    } else {
      // Assembles a terminal java process and runs it
      int exitValue = builder.build().start().waitFor();

      if (exitValue != 0) {
        throw new RuntimeException("Process has been finished with exit value - " + exitValue);
      } else {
        LOG.info("Process has been finished with exit value - {}", exitValue);
      }
    }
  }

  private void runDistributed(PipelinesVerbatimMessage message, ProcessRunnerBuilder builder) throws Exception {
    long recordsNumber = getRecordNumber(message);
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);

    builder.sparkParallelism(computeSparkParallelism(sparkExecutorNumbers))
        .sparkExecutorMemory(computeSparkExecutorMemory(sparkExecutorNumbers))
        .sparkExecutorNumbers(sparkExecutorNumbers);

    // Assembles a terminal java process and runs it
    int exitValue = builder.build().start().waitFor();

    if (exitValue != 0) {
      throw new RuntimeException("Process has been finished with exit value - " + exitValue);
    } else {
      LOG.info("Process has been finished with exit value - {}", exitValue);
    }
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   * Remember YARN will create the same number of files
   */
  private int computeSparkParallelism(int executorNumbers) {
    int count = executorNumbers * config.sparkExecutorCores * 2;

    if(count < config.sparkParallelismMin) {
      return config.sparkParallelismMin;
    }
    if(count > config.sparkParallelismMax){
      return config.sparkParallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and
   * max is config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {

    if(sparkExecutorNumbers < config.sparkExecutorMemoryGbMin) {
      return config.sparkExecutorMemoryGbMin + "G";
    }
    if(sparkExecutorNumbers > config.sparkExecutorMemoryGbMax){
      return config.sparkExecutorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and
   * max is config.sparkExecutorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers = (int) Math.ceil(recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread));
    if(sparkExecutorNumbers < config.sparkExecutorNumbersMin) {
      return config.sparkExecutorNumbersMin;
    }
    if(sparkExecutorNumbers > config.sparkExecutorNumbersMax){
      return config.sparkExecutorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /**
   * Reads number of records from the message or archive-to-avro metadata file
   */
  private long getRecordNumber(PipelinesVerbatimMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);
    LOG.info("Getting records number from the file - {}", metaPath);

    Long messageNumber = message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null
        ? message.getValidationResult().getNumberOfRecords() : null;
    String fileNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);

    if (messageNumber == null && (fileNumber == null || fileNumber.isEmpty())) {
      throw new IllegalArgumentException( "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (messageNumber == null) {
      return Long.parseLong(fileNumber);
    }

    if (fileNumber == null || fileNumber.isEmpty()) {
      return messageNumber;
    }

    return messageNumber > Long.parseLong(fileNumber) ? messageNumber : Long.parseLong(fileNumber);
  }

  /**
   * Checks if the directory exists
   */
  private boolean pathExists(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String path = String.join("/", config.repositoryPath, datasetId, attempt, Interpretation.DIRECTORY_NAME);
    try {
      return HdfsUtils.exists(config.hdfsSiteConfig, path);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Finds the latest attempt number in HDFS
   */
  private Integer getLatestAttempt(PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String path = String.join("/", config.repositoryPath, datasetId);
    try {
      return HdfsUtils.getSubDirList(config.hdfsSiteConfig, path)
          .stream()
          .map(y -> y.getPath().getName())
          .filter(x -> x.chars().allMatch(Character::isDigit))
          .mapToInt(Integer::valueOf)
          .max()
          .orElseThrow(() -> new RuntimeException("Can't find the maximum attempt"));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier(UUID datasetId, int attempt) {
    return () ->
        HdfsUtils.readMetricsFromMetaFile(
            config.hdfsSiteConfig,
            buildOutputPathAsString(
                config.repositoryPath,
                datasetId.toString(),
                String.valueOf(attempt),
                config.metaFileName));
  }
}
