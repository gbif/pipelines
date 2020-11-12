package org.gbif.pipelines.crawler.xml;

import static org.gbif.pipelines.common.utils.HdfsUtils.buildOutputPath;
import static org.gbif.pipelines.common.utils.PathUtil.buildXmlInputPath;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.crawler.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/** Call back which is called when the {@link PipelinesXmlMessage} is received. */
@Slf4j
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage>
    implements StepHandler<PipelinesXmlMessage, PipelinesVerbatimMessage> {

  public static final int SKIP_RECORDS_CHECK = -1;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;
  private final ExecutorService executor;
  private final HttpClient httpClient;

  public XmlToAvroCallback(
      XmlToAvroConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client,
      ExecutorService executor,
      HttpClient httpClient) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
    this.executor = executor;
    this.httpClient = httpClient;
  }

  @Override
  public void handleMessage(PipelinesXmlMessage message) {
    PipelinesCallback.<PipelinesXmlMessage, PipelinesVerbatimMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(StepType.XML_TO_VERBATIM)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  public Runnable createRunnable(UUID datasetId, String attempt, int expectedRecords) {
    return () -> {

      // Build and checks existence of DwC Archive
      Path inputPath =
          buildXmlInputPath(
              config.archiveRepository, config.archiveRepositorySubdir, datasetId, attempt);
      log.info("XML path - {}", inputPath);

      // Builds export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Builds metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      boolean isConverted =
          XmlToAvroConverter.create()
              .executor(executor)
              .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
              .syncInterval(config.avroConfig.syncInterval)
              .hdfsSiteConfig(config.stepConfig.hdfsSiteConfig)
              .coreSiteConfig(config.stepConfig.coreSiteConfig)
              .inputPath(inputPath)
              .outputPath(outputPath)
              .metaPath(metaPath)
              .convert();

      if (isConverted) {
        checkRecordsSize(datasetId.toString(), attempt, expectedRecords);
      } else {
        throw new IllegalArgumentException(
            "Dataset - "
                + datasetId
                + " attempt - "
                + attempt
                + " avro was deleted, cause it is empty! Please check XML files in the directory -> "
                + inputPath);
      }
    };
  }

  @Override
  public Runnable createRunnable(PipelinesXmlMessage message) {
    UUID datasetId = message.getDatasetUuid();
    String attempt = message.getAttempt().toString();
    return createRunnable(datasetId, attempt, message.getTotalRecordCount());
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesXmlMessage message) {

    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(
          new HashSet<>(
              Arrays.asList(
                  StepType.XML_TO_VERBATIM.name(),
                  StepType.VERBATIM_TO_INTERPRETED.name(),
                  StepType.INTERPRETED_TO_INDEX.name(),
                  StepType.HDFS_VIEW.name(),
                  StepType.FRAGMENTER.name())));
    }

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        config.interpretTypes,
        message.getPipelineSteps(),
        message.getEndpointType());
  }

  @Override
  public boolean isMessageCorrect(PipelinesXmlMessage message) {
    return Platform.PIPELINES.equivalent(message.getPlatform())
        && message.getTotalRecordCount() != 0
        && message.getReason() == FinishReason.NORMAL;
  }

  @SneakyThrows
  private void checkRecordsSize(String datasetId, String attempt, int expectedRecords) {
    if (expectedRecords == SKIP_RECORDS_CHECK || httpClient == null) {
      return;
    }
    int currentSize = getIndexSize(config, httpClient, datasetId);
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);
    String fileNumber;
    try {
      fileNumber =
          HdfsUtils.getValueByKey(
              config.stepConfig.hdfsSiteConfig,
              config.stepConfig.coreSiteConfig,
              metaPath,
              Metrics.ARCHIVE_TO_ER_COUNT);
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    if (fileNumber == null || fileNumber.isEmpty()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }
    double recordsNumber = Double.parseDouble(fileNumber);
    if (currentSize > 0) {
      double persentage = recordsNumber * 100 / (double) currentSize;
      log.info("The dataset conversion from xml to avro got {}% of records", persentage);
      if (persentage < 70d) {
        throw new IllegalArgumentException(
            "Dataset - "
                + datasetId
                + " attempt - "
                + attempt
                + " the dataset conversion from xml to avro got less 80% of records");
      }
    }
  }

  /** Get number of record using Occurrence API */
  private int getIndexSize(XmlToAvroConfiguration config, HttpClient httpClient, String datasetId)
      throws IOException {
    String url =
        config.stepConfig.registry.wsUrl + "/occurrence/search?limit=0&datasetKey=" + datasetId;
    HttpUriRequest httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException(
          "Occurrence search API exception " + response.getStatusLine().getReasonPhrase());
    }
    return MAPPER.readTree(response.getEntity().getContent()).findValue("count").asInt();
  }
}
