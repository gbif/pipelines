package org.gbif.pipelines.crawler.indexing;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.crawler.indexing.ProcessRunnerBuilder.ProcessRunnerBuilderBuilder;
import org.gbif.pipelines.crawler.interpret.InterpreterConfiguration;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
@Slf4j
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesIndexedMessage> {

  private static final StepType TYPE = StepType.INTERPRETED_TO_INDEX;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final DatasetService datasetService;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final PipelinesHistoryWsClient client;
  private final ExecutorService executor;

  public IndexingCallback(IndexingConfiguration config, MessagePublisher publisher,
      DatasetService datasetService, CuratorFramework curator, HttpClient httpClient,
      PipelinesHistoryWsClient client, ExecutorService executor) {
    this.config = config;
    this.publisher = publisher;
    this.datasetService = datasetService;
    this.curator = curator;
    this.httpClient = httpClient;
    this.client = client;
    this.executor = executor;
  }

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesIndexedMessage>builder()
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

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link IndexingConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    if (message.getOnlyForStep() != null && !message.getOnlyForStep().equalsIgnoreCase(TYPE.name())) {
      return false;
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {
        long recordsNumber = getRecordNumber(message);

        String indexName = computeIndexName(message, recordsNumber);
        int numberOfShards = computeNumberOfShards(indexName, recordsNumber);

        ProcessRunnerBuilderBuilder builder = ProcessRunnerBuilder.builder()
            .config(config)
            .message(message)
            .esIndexName(indexName)
            .esAlias(config.indexAlias)
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
        throw new IllegalStateException("Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }

    };
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps()
    );
  }

  private void runLocal(ProcessRunnerBuilderBuilder builder) throws IOException, InterruptedException {
    if (config.standaloneUseJava) {
      InterpretedToEsIndexExtendedPipeline.run(builder.build().buildOptions(), executor);
    } else {
      // Assembles a terminal java process and runs it
      int exitValue = builder.build().get().start().waitFor();

      if (exitValue != 0) {
        throw new IllegalStateException("Process has been finished with exit value - " + exitValue);
      } else {
        log.info("Process has been finished with exit value - {}", exitValue);
      }
    }
  }

  private void runDistributed(PipelinesInterpretedMessage message, ProcessRunnerBuilderBuilder builder,
      long recordsNumber) throws IOException, InterruptedException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);

    builder.sparkParallelism(computeSparkParallelism(datasetId, attempt))
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
   * Computes the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   */
  private int computeSparkParallelism(String datasetId, String attempt) throws IOException {
    // Chooses a runner type by calculating number of files
    String basic = RecordType.BASIC.name().toLowerCase();
    String directoryName = Interpretation.DIRECTORY_NAME;
    String basicPath = String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, directoryName, basic);
    int count = HdfsUtils.getFileCount(basicPath, config.stepConfig.hdfsSiteConfig);
    if (count < config.sparkParallelismMin) {
      return config.sparkParallelismMin;
    }
    if (count > config.sparkParallelismMax) {
      return config.sparkParallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and
   * max is config.sparkExecutorMemoryGbMax
   */
  private String computeSparkExecutorMemory(int sparkExecutorNumbers) {
    int size = sparkExecutorNumbers * 2;
    if (size < config.sparkExecutorMemoryGbMin) {
      return config.sparkExecutorMemoryGbMin + "G";
    }
    if (size > config.sparkExecutorMemoryGbMax) {
      return config.sparkExecutorMemoryGbMax + "G";
    }
    return size + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkExecutorNumbersMin and
   * max is config.sparkExecutorNumbersMax
   * <p>
   * 500_000d is records per executor
   */
  private int computeSparkExecutorNumbers(long recordsNumber) {
    int sparkExecutorNumbers =
        (int) Math.ceil((double) recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread));
    if (sparkExecutorNumbers < config.sparkExecutorNumbersMin) {
      return config.sparkExecutorNumbersMin;
    }
    if (sparkExecutorNumbers > config.sparkExecutorNumbersMax) {
      return config.sparkExecutorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /**
   * Computes the name for ES index:
   * Case 1 - Independent index for datasets where number of records more than config.indexIndepRecord
   * Case 2 - Default static index name for datasets where last changed date more than
   * config.indexDefStaticDateDurationDd
   * Case 3 - Default dynamic index name for all other datasets
   */
  private String computeIndexName(PipelinesInterpretedMessage message, long recordsNumber) throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String prefix = message.getResetPrefix();

    // Independent index for datasets where number of records more than config.indexIndepRecord
    String idxName;

    if (recordsNumber >= config.indexIndepRecord) {
      idxName = datasetId + "_" + message.getAttempt();
      idxName = prefix == null ? idxName : idxName + "_" + prefix;
      idxName = idxName + "_" + Instant.now().toEpochMilli();
      log.info("ES Index name - {}, recordsNumber - {}", idxName, recordsNumber);
      return idxName;
    }

    // Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
    Date lastChangedDate = getLastChangedDate(datasetId);

    long diffInMillies = Math.abs(new Date().getTime() - lastChangedDate.getTime());
    long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

    if (diff >= config.indexDefStaticDateDurationDd) {
      String esPr = prefix == null ? config.indexDefStaticPrefixName : config.indexDefStaticPrefixName + "_" + prefix;
      idxName = getIndexName(esPr).orElse(esPr + "_" + Instant.now().toEpochMilli());
      log.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
      return idxName;
    }

    // Default dynamic index name for all other datasets
    String esPr = prefix == null ? config.indexDefDynamicPrefixName : config.indexDefDynamicPrefixName + "_" + prefix;
    idxName = getIndexName(esPr).orElse(esPr + "_" + Instant.now().toEpochMilli());
    log.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
    return idxName;
  }

  /**
   * Computes number of index shards:
   * 1) in case of default index -> config.indexDefSize / config.indexRecordsPerShard
   * 2) in case of independent index -> recordsNumber / config.indexRecordsPerShard
   */
  private int computeNumberOfShards(String indexName, long recordsNumber) {
    if (indexName.startsWith(config.indexDefDynamicPrefixName)
        || indexName.startsWith(config.indexDefStaticPrefixName)) {
      return (int) Math.ceil((double) config.indexDefSize / (double) config.indexRecordsPerShard);
    }

    double shards = (double) recordsNumber / (double) config.indexRecordsPerShard;
    shards = Math.max(shards, 1d);
    boolean isCeil = (shards - Math.floor(shards)) > 0.25d;
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
  }

  /**
   * Uses Registry to ask the last changed date for a dataset
   */
  private Date getLastChangedDate(String datasetId) {
    Dataset dataset = datasetService.get(UUID.fromString(datasetId));
    return dataset.getModified();
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains attempted records
   * count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    String metaPath = String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfRecords();
    String fileNumber =
        HdfsUtils.getValueByKey(config.stepConfig.hdfsSiteConfig, metaPath, Metrics.BASIC_RECORDS_COUNT + "Attempted");

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

  /**
   * Returns index name by index prefix where number of records is less than configured
   */
  private Optional<String> getIndexName(String prefix) throws IOException {
    String url = String.format(config.esIndexCatUrl, prefix);
    HttpUriRequest httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("ES _cat API exception " + response.getStatusLine().getReasonPhrase());
    }
    List<EsCatIndex> indices =
        MAPPER.readValue(response.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {});
    if (!indices.isEmpty() && indices.get(0).getCount() <= config.indexDefNewIfSize) {
      return Optional.of(indices.get(0).getName());
    }
    return Optional.empty();
  }

}
