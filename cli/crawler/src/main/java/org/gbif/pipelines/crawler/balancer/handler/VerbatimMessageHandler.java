package org.gbif.pipelines.crawler.balancer.handler;

import java.io.IOException;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.balancer.BalancerConfiguration;
import org.gbif.pipelines.crawler.dwca.DwcaToAvroConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;

/**
 * Populates and sends the {@link PipelinesVerbatimMessage} message, the main method
 * is {@link VerbatimMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimMessageHandler {

  /**
   * Main handler, basically computes the runner type and sends to the same consumer
   */
  public static void handle(BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesVerbatimMessage - {}", message);

    // Populate message fields
    ObjectMapper mapper = new ObjectMapper();
    PipelinesVerbatimMessage m = mapper.readValue(message.getPayload(), PipelinesVerbatimMessage.class);

    if (m.getAttempt() == null) {
      Integer attempt = getLatestAttempt(config, m);
      log.info("Message attempt is null, HDFS parsed attempt - {}", attempt);
      m.setAttempt(attempt);
    }

    long recordsNumber = getRecordNumber(config, m);
    String runner = computeRunner(config, m, recordsNumber).name();

    ValidationResult result = m.getValidationResult();
    if (result.getNumberOfRecords() == null) {
      result.setNumberOfRecords(recordsNumber);
    }

    PipelinesVerbatimMessage outputMessage =
        new PipelinesVerbatimMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getInterpretTypes(),
            m.getPipelineSteps(),
            runner,
            m.getEndpointType(),
            m.getExtraPath(),
            result,
            m.getResetPrefix(),
            m.getExecutionId()
        );

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }

  /**
   * Computes runner type:
   * Strategy 1 - Chooses a runner type by number of records in a dataset
   * Strategy 2 - Chooses a runner type by calculating verbatim.avro file size
   */
  private static StepRunner computeRunner(BalancerConfiguration config, PipelinesVerbatimMessage message,
      long recordsNumber) throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = String.valueOf(message.getAttempt());

    StepRunner runner;

    if (message.getInterpretTypes().size() == 1 && message.getInterpretTypes().contains(METADATA.name())) {
      runner = StepRunner.STANDALONE;
      log.info("Interpret type is METADATA only, Spark Runner type - {}", runner);
      return runner;
    }

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      runner = recordsNumber >= config.switchRecordsNumber ? StepRunner.DISTRIBUTED : StepRunner.STANDALONE;
      log.info("Records number - {}, Spark Runner type - {}", recordsNumber, runner);
      return runner;
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    String verbatimPath = String.join("/", config.repositoryPath, datasetId, attempt, verbatim);
    long fileSizeByte = HdfsUtils.getFileSizeByte(config.hdfsSiteConfig, config.coreSiteConfig, verbatimPath);
    if (fileSizeByte > 0) {
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      runner = fileSizeByte > switchFileSizeByte ? StepRunner.DISTRIBUTED : StepRunner.STANDALONE;
      log.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
      return runner;
    }

    throw new IllegalStateException("Runner computation is failed " + datasetId);
  }

  /**
   * Reads number of records from a archive-to-avro metadata file
   */
  private static long getRecordNumber(BalancerConfiguration config, PipelinesVerbatimMessage message)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath = String.join("/", config.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    Long messageNumber =
        message.getValidationResult() != null && message.getValidationResult().getNumberOfRecords() != null
            ? message.getValidationResult().getNumberOfRecords() : null;
    String fileNumber =
        HdfsUtils.getValueByKey(
            config.hdfsSiteConfig, config.coreSiteConfig, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);

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
   * Finds the latest attempt number in HDFS
   */
  @SneakyThrows
  private static Integer getLatestAttempt(BalancerConfiguration config, PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    String path = String.join("/", config.repositoryPath, datasetId);
    log.info("Parsing HDFS directory - {}", path);
    return HdfsUtils.getSubDirList(config.hdfsSiteConfig, config.coreSiteConfig, path)
        .stream()
        .map(y -> y.getPath().getName())
        .filter(x -> x.chars().allMatch(Character::isDigit))
        .mapToInt(Integer::valueOf)
        .max()
        .orElseThrow(() -> new IllegalStateException("Can't find the maximum attempt"));
  }

}
