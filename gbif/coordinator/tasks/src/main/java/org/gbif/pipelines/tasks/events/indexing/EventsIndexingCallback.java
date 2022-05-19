package org.gbif.pipelines.tasks.events.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.EsCatIndex;

/** Callback which is called when the {@link PipelinesEventsMessage} is received. */
@Slf4j
public class EventsIndexingCallback
    extends AbstractMessageCallback<PipelinesEventsInterpretedMessage>
    implements StepHandler<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final EventsIndexingConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final HdfsConfigs hdfsConfigs;

  public EventsIndexingCallback(
      EventsIndexingConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      HttpClient httpClient) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    this.httpClient = httpClient;
  }

  @Override
  public void handleMessage(PipelinesEventsInterpretedMessage message) {
    StepType type = StepType.EVENTS_INTERPRETED_TO_INDEX;
    PipelinesCallback.<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage>builder()
        .config(config)
        .curator(curator)
        .stepType(type)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesEventsInterpretedMessage message) {
    return message.getNumberOfEventRecords() > 0;
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs interpreted-to-index
   * pipeline
   */
  @Override
  public Runnable createRunnable(PipelinesEventsInterpretedMessage message) {
    return () -> {
      try {
        long recordsNumber = getRecordNumber(message);

        String indexName = computeIndexName(message, recordsNumber);
        int numberOfShards = computeNumberOfShards(recordsNumber);

        ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder =
            ProcessRunnerBuilder.builder()
                .config(config)
                .message(message)
                .esIndexName(indexName)
                .esAlias(config.indexConfig.occurrenceAlias)
                .esShardsNumber(numberOfShards);

        log.info("Start the process. Message - {}", message);
        runDistributed(message, builder, recordsNumber);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  @Override
  public PipelinesEventsIndexedMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getRunner());
  }

  private void runDistributed(
      PipelinesEventsInterpretedMessage message,
      ProcessRunnerBuilder.ProcessRunnerBuilderBuilder builder,
      long recordsNumber)
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
    String eventCore = RecordType.EVENT_CORE.name().toLowerCase();
    String eventsPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, eventCore);
    int count = HdfsUtils.getFileCount(hdfsConfigs, eventsPath);
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
  private String computeIndexName(PipelinesEventsInterpretedMessage message, long recordsNumber)
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

  private int computeNumberOfShards(long recordsNumber) {
    double shards = recordsNumber / (double) config.indexConfig.recordsPerShard;
    shards = Math.max(shards, 1d);
    boolean isCeil = (shards - Math.floor(shards)) > 0.25d;
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private long getRecordNumber(PipelinesEventsInterpretedMessage message) throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new EventsInterpretationConfiguration().metaFileName;
    String metaPath =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
            datasetId,
            attempt,
            DwcTerm.Event.simpleName().toLowerCase(),
            metaFileName);

    Long messageNumber = message.getNumberOfEventRecords();
    // TODO: check what metric to read
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            hdfsConfigs, metaPath, Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED);

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
