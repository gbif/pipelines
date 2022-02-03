package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Conversion;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;
import org.gbif.pipelines.tasks.interpret.InterpreterConfiguration;

/**
 * Populates and sends the {@link PipelinesInterpretedMessage} message, the main method is {@link
 * InterpretedMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesInterpretedMessage - {}", message);

    // Populate message fields
    PipelinesInterpretedMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesInterpretedMessage.class);

    long recordsNumber = getRecordNumber(config, m);

    String runner = computeRunner(config, m, recordsNumber).name();

    PipelinesInterpretedMessage outputMessage =
        new PipelinesInterpretedMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            recordsNumber,
            runner,
            m.isRepeatAttempt(),
            m.getResetPrefix(),
            m.getOnlyForStep(),
            m.getExecutionId(),
            m.getEndpointType(),
            m.getValidationResult(),
            m.getInterpretTypes(),
            m.isValidator());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }

  /**
   * Computes runner type: Strategy 1 - Chooses a runner type by number of records in a dataset
   * Strategy 2 - Chooses a runner type by calculating verbatim.avro file size
   */
  private static StepRunner computeRunner(
      BalancerConfiguration config, PipelinesInterpretedMessage message, long recordsNumber)
      throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = message.getAttempt().toString();

    StepRunner runner;

    // Strategy 1: Chooses a runner type by number of records in a dataset
    if (recordsNumber > 0) {
      runner =
          recordsNumber >= config.switchRecordsNumber
              ? StepRunner.DISTRIBUTED
              : StepRunner.STANDALONE;
      log.info("Records number - {}, Spark Runner type - {}", recordsNumber, runner);
      return runner;
    }

    // Strategy 2: Chooses a runner type by calculating verbatim.avro file size
    String verbatim = Conversion.FILE_NAME + Pipeline.AVRO_EXTENSION;
    StepConfiguration stepConfig = config.stepConfig;
    String repositoryPath =
        message.isValidator() ? config.validatorRepositoryPath : stepConfig.repositoryPath;
    String verbatimPath = String.join("/", repositoryPath, datasetId, attempt, verbatim);
    long fileSizeByte =
        HdfsUtils.getFileSizeByte(
            stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig, verbatimPath);
    if (fileSizeByte > 0) {
      long switchFileSizeByte = config.switchFileSizeMb * 1024L * 1024L;
      runner = fileSizeByte > switchFileSizeByte ? StepRunner.DISTRIBUTED : StepRunner.STANDALONE;
      log.info("File size - {}, Spark Runner type - {}", fileSizeByte, runner);
      return runner;
    }

    throw new IllegalStateException("Runner computation is failed " + datasetId);
  }

  /**
   * Reads number of records from a archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  private static long getRecordNumber(
      BalancerConfiguration config, PipelinesInterpretedMessage message) throws IOException {

    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    StepConfiguration stepConfig = config.stepConfig;
    String repositoryPath =
        message.isValidator() ? config.validatorRepositoryPath : stepConfig.repositoryPath;
    String metaPath = String.join("/", repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = message.getNumberOfRecords();
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            stepConfig.hdfsSiteConfig,
            stepConfig.coreSiteConfig,
            metaPath,
            Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED);

    // Fail if fileNumber is null
    if (!message.isValidator()) {
      Set<String> types = message.getInterpretTypes();
      boolean isCorrectType = types.contains(ALL.name()) || types.contains(BASIC.name());
      boolean noFileRecords = !fileNumber.isPresent() || fileNumber.get() == 0L;
      if (isCorrectType && noFileRecords) {
        throw new IllegalArgumentException(
            "Basic records must be interpreted, but fileNumber is null or 0, please validate the archive!");
      }
    }

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
}
