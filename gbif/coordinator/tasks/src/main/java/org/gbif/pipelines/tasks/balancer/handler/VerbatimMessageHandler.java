package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;
// import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;

/**
 * Populates and sends the {@link PipelinesVerbatimMessage} message, the main method is {@link
 * VerbatimMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesVerbatimMessage - {}", message);

    // Populate message fields
    PipelinesVerbatimMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesVerbatimMessage.class);

    if (m.getAttempt() == null) {
      Integer attempt = getLatestAttempt(config, m);
      log.info("Message attempt is null, HDFS parsed attempt - {}", attempt);
      m.setAttempt(attempt);
    }

    // case of sampling event dataset without occurrences. We only run the events pipelines
    if (config.stepConfig.eventsEnabled
        && m.getDatasetType() == DatasetType.SAMPLING_EVENT
        && (m.getValidationResult().getNumberOfRecords() == null
            || m.getValidationResult().getNumberOfRecords() == 0)
        && m.getValidationResult().getNumberOfEventRecords() != null
        && m.getValidationResult().getNumberOfEventRecords() > 0) {
      Set<String> interpretationTypes = new HashSet<>(m.getInterpretTypes());
      interpretationTypes.add(RecordType.EVENT.name());
      interpretationTypes.remove(RecordType.OCCURRENCE.name());

      PipelinesEventsMessage eventsMessage =
          new PipelinesEventsMessage(
              m.getDatasetUuid(),
              m.getAttempt(),
              m.getPipelineSteps(),
              m.getValidationResult().getNumberOfEventRecords(),
              m.getValidationResult().getNumberOfRecords(),
              StepRunner.DISTRIBUTED.name(),
              false,
              m.getResetPrefix(),
              m.getExecutionId(),
              m.getEndpointType(),
              m.getValidationResult(),
              interpretationTypes,
              DatasetType.SAMPLING_EVENT);

      publisher.send(eventsMessage);
      log.info("The events message has been sent - {}", eventsMessage);
    } else {

      Optional<Long> occCount =
          getRecordNumber(
              config, m, new DwcaToAvroConfiguration().metaFileName, Metrics.ARCHIVE_TO_OCC_COUNT);

      Optional<Long> erCount =
          getRecordNumber(
              config, m, new DwcaToAvroConfiguration().metaFileName, Metrics.ARCHIVE_TO_ER_COUNT);

      Optional<Long> uniqueIdsCount =
          getRecordNumber(
              config,
              m,
              null,
              //              new IdentifierConfiguration().metaFileName,
              Metrics.UNIQUE_IDS_COUNT + Metrics.ATTEMPTED);

      log.info("The record numbers - occ: {}, er: {}, ids: {}", occCount, erCount, uniqueIdsCount);

      Optional<Long> recordNumberOpt =
          Stream.of(occCount, erCount, uniqueIdsCount)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .max(Long::compareTo);

      log.info("Used record number - {}", recordNumberOpt);

      if (recordNumberOpt.isEmpty()) {
        throw new IllegalArgumentException(
            "Can't find information about amount of records in MQ of meta files");
      }

      long recordsNumber = recordNumberOpt.get();

      String runner = computeRunner(config, m).name();

      ValidationResult result = m.getValidationResult();
      if (result.getNumberOfRecords() == null || isValidator(m.getPipelineSteps())) {
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
              m.getExecutionId(),
              m.getDatasetType());

      publisher.send(outputMessage);
      log.info("The message has been sent - {}", outputMessage);
    }
  }

  /**
   * Computes runner type: Strategy 1 - Chooses a runner type by number of records in a dataset
   * Strategy 2 - Chooses a runner type by calculating verbatim.avro file size
   */
  public static StepRunner computeRunner(BalancerConfiguration config, PipelineBasedMessage message)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = String.valueOf(message.getAttempt());

    StepRunner runner;

    // Chooses a runner type by calculating verbatim.avro file size
    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    StepConfiguration stepConfig = config.stepConfig;
    String repositoryPath =
        isValidator(message.getPipelineSteps())
            ? config.validatorRepositoryPath
            : stepConfig.repositoryPath;
    String verbatimPath = String.join("/", repositoryPath, datasetId, attempt, verbatim);
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);
    long fileSizeByte = HdfsUtils.getFileSizeByte(hdfsConfigs, verbatimPath);
    if (fileSizeByte > 0) {
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      runner = fileSizeByte > switchFileSizeByte ? StepRunner.DISTRIBUTED : StepRunner.STANDALONE;
      log.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
      return runner;
    }

    throw new IllegalStateException("Runner computation is failed " + datasetId);
  }

  /** Reads number of records from a metadata file */
  private static Optional<Long> getRecordNumber(
      BalancerConfiguration config,
      PipelinesVerbatimMessage message,
      String metaFileName,
      String metricName)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    StepConfiguration stepConfig = config.stepConfig;
    String repositoryPath =
        isValidator(message.getPipelineSteps())
            ? config.validatorRepositoryPath
            : stepConfig.repositoryPath;
    String metaPath = String.join("/", repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    Long messageNumber =
        message.getValidationResult() != null
                && message.getValidationResult().getNumberOfRecords() != null
            ? message.getValidationResult().getNumberOfRecords()
            : null;

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);
    Optional<Long> fileNumber = HdfsUtils.getLongByKey(hdfsConfigs, metaPath, metricName);

    if (messageNumber == null && fileNumber.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(fileNumber.orElse(messageNumber));
  }

  /** Finds the latest attempt number in HDFS */
  @SneakyThrows
  private static Integer getLatestAttempt(
      BalancerConfiguration config, PipelinesVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    StepConfiguration stepConfig = config.stepConfig;
    String repositoryPath =
        isValidator(message.getPipelineSteps())
            ? config.validatorRepositoryPath
            : stepConfig.repositoryPath;
    String path = String.join("/", repositoryPath, datasetId);

    log.info("Parsing HDFS directory - {}", path);
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);
    return HdfsUtils.getSubDirList(hdfsConfigs, path).stream()
        .map(y -> y.getPath().getName())
        .filter(x -> x.chars().allMatch(Character::isDigit))
        .mapToInt(Integer::valueOf)
        .max()
        .orElseThrow(() -> new IllegalStateException("Can't find the maximum attempt"));
  }
}
