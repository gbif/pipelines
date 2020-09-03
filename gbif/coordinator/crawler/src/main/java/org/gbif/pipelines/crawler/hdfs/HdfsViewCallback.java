package org.gbif.pipelines.crawler.hdfs;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.crawler.hdfs.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.crawler.interpret.InterpreterConfiguration;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
@Slf4j
public class HdfsViewCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesHdfsViewBuiltMessage> {

  private static final StepType TYPE = StepType.HDFS_VIEW;

  private final HdfsViewConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;
  private final ExecutorService executor;

  public HdfsViewCallback(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client,
      ExecutorService executor) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
    this.executor = executor;
  }

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesHdfsViewBuiltMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  /** Main message processing logic, creates a terminal java process, which runs */
  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {
        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .config(config)
                .message(message)
                .numberOfShards(computeNumberOfShards(message));

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        log.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder);
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
  public PipelinesHdfsViewBuiltMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesHdfsViewBuiltMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    if (message.getOnlyForStep() != null
        && !message.getOnlyForStep().equalsIgnoreCase(TYPE.name())) {
      return false;
    }
    return config.processRunner.equals(message.getRunner());
  }

  private void runLocal(ProcessRunnerBuilderBuilder builder) {
    InterpretedToHdfsViewPipeline.run(builder.build().buildOptions(), executor);
  }

  private void runDistributed(
      PipelinesInterpretedMessage message, ProcessRunnerBuilderBuilder builder)
      throws IOException, InterruptedException {

    long recordNumber = getRecordNumber(message);
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordNumber);

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
    int count = executorNumbers * config.sparkExecutorCores * 2;

    if (count < config.sparkParallelismMin) {
      return config.sparkParallelismMin;
    }
    if (count > config.sparkParallelismMax) {
      return config.sparkParallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and max is
   * config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {

    if (sparkExecutorNumbers < config.sparkExecutorMemoryGbMin) {
      return config.sparkExecutorMemoryGbMin + "G";
    }
    if (sparkExecutorNumbers > config.sparkExecutorMemoryGbMax) {
      return config.sparkExecutorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and max is
   * config.sparkExecutorNumbersMax
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers =
        (int)
            Math.ceil(
                recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread * 1f));
    if (sparkExecutorNumbers < config.sparkExecutorNumbersMin) {
      return config.sparkExecutorNumbersMin;
    }
    if (sparkExecutorNumbers > config.sparkExecutorNumbersMax) {
      return config.sparkExecutorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfRecords();
    String fileNumber =
        HdfsUtils.getValueByKey(
            config.stepConfig.hdfsSiteConfig,
            config.stepConfig.coreSiteConfig,
            metaPath,
            Metrics.BASIC_RECORDS_COUNT + "Attempted");

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

  private int computeNumberOfShards(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String dirPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, DIRECTORY_NAME);
    long sizeByte =
        HdfsUtils.getFileSizeByte(
            config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig, dirPath);
    if (sizeByte == -1d) {
      throw new IllegalArgumentException(
          "Please check interpretation source directory! - " + dirPath);
    }
    long sizeExpected = config.hdfsAvroExpectedFileSizeInMb * 1048576L; // 1024 * 1024
    double numberOfShards = (sizeByte * config.hdfsAvroCoefficientRatio / 100f) / sizeExpected;
    double numberOfShardsFloor = Math.floor(numberOfShards);
    numberOfShards =
        numberOfShards - numberOfShardsFloor > 0.5d ? numberOfShardsFloor + 1 : numberOfShardsFloor;
    return numberOfShards <= 0 ? 1 : (int) numberOfShards;
  }
}
