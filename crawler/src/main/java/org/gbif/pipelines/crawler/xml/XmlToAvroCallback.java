package org.gbif.pipelines.crawler.xml;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelineCallback;
import org.gbif.pipelines.crawler.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.avro.file.CodecFactory;
import org.apache.curator.framework.CuratorFramework;

import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.utils.HdfsUtils.buildOutputPath;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 */
@Slf4j
public class XmlToAvroCallback extends PipelineCallback<PipelinesXmlMessage, PipelinesVerbatimMessage> {

  private static final String TAR_EXT = ".tar.xz";

  public static final int SKIP_RECORDS_CHECK = -1;

  private final XmlToAvroConfiguration config;
  private final ExecutorService executor;

  public XmlToAvroCallback(XmlToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient client, @NonNull ExecutorService executor) {
    super(StepType.XML_TO_VERBATIM, curator, publisher, client, config);
    this.config = config;
    this.executor = executor;
  }

  public static Runnable createRunnable(XmlToAvroConfiguration config, UUID datasetId, String attempt,
      ExecutorService executor, int expectedRecords) {
    return () -> {

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config, datasetId, attempt);
      log.info("XML path - {}", inputPath);

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

  @Override
  protected Runnable createRunnable(PipelinesXmlMessage message) {
    UUID datasetId = message.getDatasetUuid();
    String attempt = message.getAttempt().toString();
    return createRunnable(config, datasetId, attempt, executor, message.getTotalRecordCount());
  }

  @Override
  protected PipelinesVerbatimMessage createOutgoingMessage(PipelinesXmlMessage message) {

    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(Sets.newHashSet(
          StepType.XML_TO_VERBATIM.name(),
          StepType.VERBATIM_TO_INTERPRETED.name(),
          StepType.INTERPRETED_TO_INDEX.name(),
          StepType.HDFS_VIEW.name()
      ));
    }

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        config.interpretTypes,
        message.getPipelineSteps(),
        message.getEndpointType()
    );
  }

  @Override
  protected boolean isMessageCorrect(PipelinesXmlMessage message) {
    return Platform.PIPELINES.equivalent(message.getPlatform())
        && message.getTotalRecordCount() != 0
        && message.getReason() == FinishReason.NORMAL;
  }

  private static void checkRecordsSize(XmlToAvroConfiguration config, String datasetId, String attempt,
      int expectedRecords) {
    if (expectedRecords == SKIP_RECORDS_CHECK) {
      return;
    }
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);
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
    log.info("The dataset conversion from xml to avro got {}% of records", persentage);
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
      log.error(ex.getMessage(), ex);
    }

    // Return general
    return directoryPath;
  }
}
