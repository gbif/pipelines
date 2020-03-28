package org.gbif.crawler.pipelines.xml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.dwca.DwcaToAvroConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.avro.file.CodecFactory;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.collect.Sets;

import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPath;
import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPathAsString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 * <p>
 * The main method is {@link XmlToAvroCallback#handleMessage}
 */
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroCallback.class);
  private static final StepType STEP = StepType.XML_TO_VERBATIM;
  private static final String TAR_EXT = ".tar.xz";

  public static final int SKIP_RECORDS_CHECK = -1;

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient historyWsClient;
  private final ExecutorService executor;

  public XmlToAvroCallback(XmlToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient historyWsClient, ExecutorService executor) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.executor = checkNotNull(executor, "executor cannot be null");
    this.publisher = publisher;
    this.historyWsClient = historyWsClient;
  }

  /**
   * Handles a MQ {@link PipelinesXmlMessage} message
   */
  @Override
  public void handleMessage(PipelinesXmlMessage message) {

    if (!Platform.PIPELINES.equivalent(message.getPlatform())) {
      LOG.info("Skip message because pipelines don't support the platform {}", message);
      return;
    }

    if(message.getTotalRecordCount() == 0){
      LOG.info("Skip empty dataset {}", message);
      return;
    }

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", message.getDatasetUuid().toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {
      LOG.info("Message handler began - {}", message);

      if (message.getReason() != FinishReason.NORMAL) {
        LOG.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      if (message.getPipelineSteps().isEmpty()) {
        message.setPipelineSteps(Sets.newHashSet(
            StepType.XML_TO_VERBATIM.name(),
            StepType.VERBATIM_TO_INTERPRETED.name(),
            StepType.INTERPRETED_TO_INDEX.name(),
            StepType.HDFS_VIEW.name()
        ));
      }

      // Common variables
      EndpointType endpointType = message.getEndpointType();
      Set<String> steps = message.getPipelineSteps();
      Runnable runnable =
          createRunnable(config, datasetId, attempt.toString(), endpointType, executor, message.getTotalRecordCount());

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType))
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
   * Main message processing logic, converts an ABCD archive to an avro file.
   */
  public static Runnable createRunnable(XmlToAvroConfiguration config, UUID datasetId, String attempt,
      EndpointType endpointType, ExecutorService executor, int expectedRecords) {
    return () -> {

      Optional.ofNullable(endpointType)
          .orElseThrow(() -> new IllegalArgumentException("endpointType can't bew NULL!"));

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config, datasetId, attempt);
      LOG.info("XML path - {}", inputPath);

      // Calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      boolean isConverted = XmlToAvroConverter.create()
          .executor(executor)
          .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .convert();

      if (isConverted) {
        checkRecordsSize(config, datasetId.toString(), attempt, expectedRecords);
      } else {
        throw new IllegalArgumentException("Dataset - " + datasetId + " attempt - " + attempt
            + " avro was deleted, cause it is empty! Please check XML files in the directory -> " + inputPath);
      }
    };
  }

  private static void checkRecordsSize(XmlToAvroConfiguration config, String datasetId, String attempt,
      int expectedRecords) {
    if (expectedRecords == SKIP_RECORDS_CHECK) {
      return;
    }
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);
    LOG.info("Getting records number from the file - {}", metaPath);
    String fileNumber;
    try {
      fileNumber = HdfsUtils.getValueByKey(config.hdfsSiteConfig, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    if (fileNumber == null || fileNumber.isEmpty()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }
    double recordsNumber = Double.parseDouble(fileNumber);
    double persentage = recordsNumber * 100 / (double) expectedRecords;
    LOG.info("The dataset conversion from xml to avro got {}% of records", persentage);
    if (persentage < 80d) {
      throw new IllegalArgumentException("Dataset - " + datasetId + " attempt - " + attempt
          + " the dataset conversion from xml to avro got less 80% of records");
    }
  }

  /**
   * Input path result example, directory - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2,
   * if directory is absent, tries check a tar archive  - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
   */
  private static Path buildInputPath(XmlToAvroConfiguration config, UUID dataSetUuid, String attempt) {

    Path directoryPath = config.archiveRepositorySubdir.stream()
        .map(subdir -> Paths.get(config.archiveRepository, subdir, dataSetUuid.toString()).toFile())
        .filter(File::exists)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Can't find directory for dataset - " + dataSetUuid))
        .toPath();

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
    Path sibling = directoryPath.resolve(String.valueOf(attempt));
    if (sibling.toFile().exists()) {
      return sibling;
    }

    // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
    sibling = directoryPath.resolve(attempt + TAR_EXT);
    if (sibling.toFile().exists()) {
      return sibling;
    }

    // TODO: Do we need this? Try to find last attempt
    try (Stream<Path> walk = Files.list(directoryPath)) {
      String parsedAttempt = walk.map(Path::toFile)
          .map(File::getName)
          .map(name -> name.replace(TAR_EXT, ""))
          .filter(f -> f.matches("[0-9]+"))
          .map(Integer::valueOf)
          .max(Integer::compareTo)
          .map(String::valueOf)
          .orElse("0");

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2
      sibling = directoryPath.resolve(parsedAttempt);
      if (sibling.toFile().exists()) {
        return sibling;
      }

      // Check dir, as an example - /mnt/auto/crawler/xml/9bed66b3-4caa-42bb-9c93-71d7ba109dad/2.tar.xz
      sibling = directoryPath.resolve(parsedAttempt + TAR_EXT);
      if (sibling.toFile().exists()) {
        return sibling;
      }
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
    }

    // Return general
    return directoryPath;
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
