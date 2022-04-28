package org.gbif.pipelines.tasks.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.indexing.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.tasks.interpret.InterpreterConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
@Slf4j
@AllArgsConstructor
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesIndexedMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final ExecutorService executor;

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    StepType type =
        message.isValidator() || config.validatorOnly
            ? StepType.VALIDATOR_INTERPRETED_TO_INDEX
            : StepType.INTERPRETED_TO_INDEX;
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesIndexedMessage>builder()
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
   * service config {@link IndexingConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message);
    }
    if ((message.isValidator() || config.validatorOnly) && config.validatorListenAllMq) {
      log.info("Running as a validator task");
      return true;
    }
    StepType type =
        message.isValidator() || config.validatorOnly
            ? StepType.VALIDATOR_INTERPRETED_TO_INDEX
            : StepType.INTERPRETED_TO_INDEX;
    if (message.getOnlyForStep() != null
        && !message.getOnlyForStep().equalsIgnoreCase(type.name())) {
      log.info("Skipping, because expected step is {}", message.getOnlyForStep());
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

        String indexName = computeIndexName(message, recordsNumber);
        int numberOfShards = computeNumberOfShards(indexName, recordsNumber);

        ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .config(config)
                .message(message)
                .esIndexName(indexName)
                .esAlias(config.indexConfig.occurrenceAlias)
                .esShardsNumber(numberOfShards);

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
        message.getEndpointType(),
        message.isValidator() || config.validatorOnly);
  }

  private void runLocal(ProcessRunnerBuilderBuilder builder) {
    InterpretedToEsIndexExtendedPipeline.run(builder.build().buildOptions(), executor);
  }

  private void runDistributed(
      PipelinesInterpretedMessage message, ProcessRunnerBuilderBuilder builder, long recordsNumber)
      throws IOException, InterruptedException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);

    builder
        .sparkParallelism(computeSparkParallelism(datasetId, attempt))
        .sparkExecutorMemory(computeSparkExecutorMemory(sparkExecutorNumbers, recordsNumber))
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
   * Computes the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax
   */
  private int computeSparkParallelism(String datasetId, String attempt) throws IOException {
    // Chooses a runner type by calculating number of files
    String basic = RecordType.BASIC.name().toLowerCase();
    String directoryName = Interpretation.DIRECTORY_NAME;
    String basicPath =
        String.join(
            "/", config.stepConfig.repositoryPath, datasetId, attempt, directoryName, basic);
    int count =
        HdfsUtils.getFileCount(
            config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig, basicPath);
    count *= 4;
    if (count < config.sparkConfig.parallelismMin) {
      return config.sparkConfig.parallelismMin;
    }
    if (count > config.sparkConfig.parallelismMax) {
      return config.sparkConfig.parallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkConfig.executorMemoryGbMin and
   * max is config.sparkConfig.executorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers, long recordsNumber) {
    int size =
        (int)
            Math.ceil(
                (double) recordsNumber
                    / (sparkExecutorNumbers * config.sparkConfig.recordsPerThread)
                    * 1.6);

    if (size < config.sparkConfig.executorMemoryGbMin) {
      return config.sparkConfig.executorMemoryGbMin + "G";
    }
    if (size > config.sparkConfig.executorMemoryGbMax) {
      return config.sparkConfig.executorMemoryGbMax + "G";
    }
    return size + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkConfig.executorNumbersMin and max
   * is config.sparkConfig.executorNumbersMax
   *
   * <p>500_000d is records per executor
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

  /**
   * Computes the name for ES index:
   *
   * <pre>
   * Case 1 - Independent index for datasets where number of records more than config.indexIndepRecord
   * Case 2 - Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
   * Case 3 - Default dynamic index name for all other datasets
   * </pre>
   */
  private String computeIndexName(PipelinesInterpretedMessage message, long recordsNumber)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();

    // Independent index for datasets where number of records more than config.indexIndepRecord
    String idxName;

    if (recordsNumber >= config.indexConfig.bigIndexIfRecordsMoreThan) {
      idxName = datasetId + "_" + message.getAttempt() + "_" + config.indexConfig.occurrenceVersion;
      idxName = idxName + "_" + Instant.now().toEpochMilli();
      log.info("ES Index name - {}, recordsNumber - {}", idxName, recordsNumber);
      return idxName;
    }

    // Default index name for all other datasets
    String esPr = config.indexConfig.defaultPrefixName + "_" + config.indexConfig.occurrenceVersion;
    idxName = getIndexName(esPr).orElse(esPr + "_" + Instant.now().toEpochMilli());
    log.info("ES Index name - {}", idxName);
    return idxName;
  }

  /**
   * Computes number of index shards:
   *
   * <pre>
   * 1) in case of default index -> config.indexDefSize / config.indexRecordsPerShard
   * 2) in case of independent index -> recordsNumber / config.indexRecordsPerShard
   * </pre>
   */
  private int computeNumberOfShards(String indexName, long recordsNumber) {
    if (indexName.startsWith(config.indexConfig.defaultPrefixName)) {
      return (int)
          Math.ceil(
              (double) config.indexConfig.defaultSize
                  / (double) config.indexConfig.recordsPerShard);
    }

    double shards = recordsNumber / (double) config.indexConfig.recordsPerShard;
    shards = Math.max(shards, 1d);
    boolean isCeil = (shards - Math.floor(shards)) > 0.25d;
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
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
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            config.stepConfig.hdfsSiteConfig,
            config.stepConfig.coreSiteConfig,
            metaPath,
            Metrics.GBIF_ID_RECORDS_COUNT + Metrics.ATTEMPTED);

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

  /** Returns index name by index prefix where number of records is less than configured */
  private Optional<String> getIndexName(String prefix) throws IOException {
    String url = String.format(config.indexConfig.defaultSmallestIndexCatUrl, prefix);
    HttpUriRequest httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("ES _cat API exception " + response.getStatusLine().getReasonPhrase());
    }
    List<EsCatIndex> indices =
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {});
    if (!indices.isEmpty() && indices.get(0).getCount() <= config.indexConfig.defaultNewIfSize) {
      return Optional.of(indices.get(0).getName());
    }
    return Optional.empty();
  }
}
