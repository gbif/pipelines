package org.gbif.crawler.pipelines.indexing;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.interpret.InterpreterConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPathAsString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 * <p>
 * The main method is {@link IndexingCallback#handleMessage}
 */
public class IndexingCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexingCallback.class);
  private static final StepType STEP = StepType.INTERPRETED_TO_INDEX;
  private static  final ObjectMapper MAPPER = new ObjectMapper();

  private final IndexingConfiguration config;
  private final MessagePublisher publisher;
  private final DatasetService datasetService;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final PipelinesHistoryWsClient historyWsClient;
  private final ExecutorService executor;

  IndexingCallback(IndexingConfiguration config, MessagePublisher publisher, DatasetService datasetService,
      CuratorFramework curator, HttpClient httpClient, PipelinesHistoryWsClient historyWsClient, ExecutorService executor) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.datasetService = checkNotNull(datasetService, "config cannot be null");
    this.publisher = publisher;
    this.httpClient = httpClient;
    this.historyWsClient = historyWsClient;
    this.executor = executor;
  }

  /**
   * Handles a MQ {@link PipelinesInterpretedMessage} message
   */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      if (!isMessageCorrect(message)) {
        LOG.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      LOG.info("Message handler began - {}", message);

      Set<String> steps = message.getPipelineSteps();
      Runnable runnable = createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesIndexedMessage(datasetId, attempt, steps))
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
   * {@link IndexingConfiguration#processRunner}
   */
  private boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    if (Strings.isNullOrEmpty(message.getRunner())) {
      throw new IllegalArgumentException("Runner can't be null or empty " + message.toString());
    }
    if (message.getOnlyForStep() != null && !message.getOnlyForStep().equalsIgnoreCase(STEP.name())) {
      return false;
    }
    return config.processRunner.equals(message.getRunner());
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index pipeline
   */
  private Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {
        long recordsNumber = getRecordNumber(message);

        String indexName = computeIndexName(message, recordsNumber);
        int numberOfShards = computeNumberOfShards(indexName, recordsNumber);

        ProcessRunnerBuilder builder = ProcessRunnerBuilder.create()
            .config(config)
            .message(message)
            .esIndexName(indexName)
            .esAlias(config.indexAlias)
            .esShardsNumber(numberOfShards);

        Predicate<StepRunner> runnerPr = sr -> config.processRunner.equalsIgnoreCase(sr.name());

        LOG.info("Start the process. Message - {}", message);
        if (runnerPr.test(StepRunner.DISTRIBUTED)) {
          runDistributed(message, builder, recordsNumber);
        } else if (runnerPr.test(StepRunner.STANDALONE)) {
          runLocal(builder);
        }
      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
        throw new IllegalStateException("Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }

    };
  }

  private void runLocal(ProcessRunnerBuilder builder) throws Exception {
    if (config.standaloneUseJava) {
      InterpretedToEsIndexExtendedPipeline.run(builder.buildOptions(), executor);
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

  private void runDistributed(PipelinesInterpretedMessage message, ProcessRunnerBuilder builder, long recordsNumber)
      throws Exception {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    int sparkExecutorNumbers = computeSparkExecutorNumbers(recordsNumber);

    builder.sparkParallelism(computeSparkParallelism(datasetId, attempt))
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
   * Computes the number of thread for spark.default.parallelism, top limit is config.sparkParallelismMax
   */
  private int computeSparkParallelism(String datasetId, String attempt) throws IOException {
    // Chooses a runner type by calculating number of files
    String basic = RecordType.BASIC.name().toLowerCase();
    String directoryName = Interpretation.DIRECTORY_NAME;
    String basicPath = String.join("/", config.repositoryPath, datasetId, attempt, directoryName, basic);
    int count = HdfsUtils.getFileCount(basicPath, config.hdfsSiteConfig);
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
    int size =  sparkExecutorNumbers * 2;
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
    int sparkExecutorNumbers = (int) Math.ceil(recordsNumber / (config.sparkExecutorCores * config.sparkRecordsPerThread));
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
      LOG.info("ES Index name - {}, recordsNumber - {}", idxName, recordsNumber);
      return idxName;
    }

    // Default static index name for datasets where last changed date more than config.indexDefStaticDateDurationDd
    Date lastChangedDate = getLastChangedDate(datasetId);

    long diffInMillies = Math.abs(new Date().getTime() - lastChangedDate.getTime());
    long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

    if (diff >= config.indexDefStaticDateDurationDd) {
      String esPr = prefix == null ? config.indexDefStaticPrefixName : config.indexDefStaticPrefixName + "_" + prefix;
      idxName = getIndexName(esPr).orElse(esPr + "_" + Instant.now().toEpochMilli());
      LOG.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
      return idxName;
    }

    // Default dynamic index name for all other datasets
    String esPr = prefix == null ? config.indexDefDynamicPrefixName : config.indexDefDynamicPrefixName + "_" + prefix;
    idxName = getIndexName(esPr).orElse(esPr + "_" + Instant.now().toEpochMilli());
    LOG.info("ES Index name - {}, lastChangedDate - {}, diff days - {}", idxName, lastChangedDate, diff);
    return idxName;
  }

  /**
   * Computes number of index shards:
   * 1) in case of default index -> config.indexDefSize / config.indexRecordsPerShard
   * 2) in case of independent index -> recordsNumber / config.indexRecordsPerShard
   */
  private int computeNumberOfShards(String indexName, long recordsNumber) {
    if (indexName.startsWith(config.indexDefDynamicPrefixName) || indexName.startsWith(config.indexDefStaticPrefixName)) {
      return (int) Math.ceil((double) config.indexDefSize / (double) config.indexRecordsPerShard);
    }

    double shards = (double) recordsNumber / (double) config.indexRecordsPerShard;
    shards = shards > 1d ? shards : 1d;
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
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfRecords();
    String fileNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.BASIC_RECORDS_COUNT + "Attempted");

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
   * Returns index name by index prefix where number of records is less than configured
   */
  private Optional<String> getIndexName(String prefix) throws IOException {
    String url = String.format(config.esIndexCatUrl, prefix);
    HttpUriRequest httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("ES _cat API exception " + response.getStatusLine().getReasonPhrase());
    }
    List<EsCatIndex> indices = MAPPER.readValue(response.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {});
    if (indices.size() != 0 && indices.get(0).getCount() <= config.indexDefNewIfSize) {
      return Optional.of(indices.get(0).getName());
    }
    return Optional.empty();
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
