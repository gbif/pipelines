package org.gbif.pipelines.tasks.verbatims.abcd;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllInterpretationAsString;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllValidatorInterpretationAsString;
import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.common.ValidatorPredicate;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallback;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Call back which is called when the {@link PipelinesXmlMessage} is received. */
@Slf4j
@Builder
public class AbcdToAvroCallback extends AbstractMessageCallback<PipelinesAbcdMessage>
    implements StepHandler<PipelinesAbcdMessage, PipelinesVerbatimMessage> {

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final DatasetClient datasetClient;
  private final XmlToAvroCallback callback;

  @Override
  public void handleMessage(PipelinesAbcdMessage message) {
    PipelinesCallback.<PipelinesAbcdMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .datasetClient(datasetClient)
        .config(config)
        .stepType(StepType.ABCD_TO_VERBATIM)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public Runnable createRunnable(PipelinesAbcdMessage message) {

    uncompress(
        Paths.get(
            config.archiveRepository,
            config.archiveRepositorySubdir,
            message.getDatasetUuid().toString() + ".abcda"),
        Paths.get(
            config.archiveRepository,
            config.archiveRepositorySubdir,
            message.getDatasetUuid().toString()));

    return callback.createRunnable(
        message.getDatasetUuid(),
        message.getAttempt().toString(),
        XmlToAvroCallback.SKIP_RECORDS_CHECK,
        ValidatorPredicate.isValidator(message.getPipelineSteps(), config.validatorOnly));
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesAbcdMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(
          new HashSet<>(
              Arrays.asList(
                  StepType.ABCD_TO_VERBATIM.name(),
                  StepType.VERBATIM_TO_IDENTIFIER.name(),
                  StepType.VERBATIM_TO_INTERPRETED.name(),
                  StepType.INTERPRETED_TO_INDEX.name(),
                  StepType.HDFS_VIEW.name(),
                  StepType.FRAGMENTER.name())));
    }

    Set<String> allInterpretationAsString =
        isValidator(message.getPipelineSteps(), config.validatorOnly)
            ? getAllValidatorInterpretationAsString()
            : getAllInterpretationAsString();

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        allInterpretationAsString,
        message.getPipelineSteps(),
        null,
        message.getEndpointType(),
        null,
        new ValidationResult(true, true, false, null, null),
        null,
        null,
        DatasetType.OCCURRENCE);
  }

  @Override
  public boolean isMessageCorrect(PipelinesAbcdMessage message) {
    return message.isModified();
  }

  @SneakyThrows
  private File[] uncompress(Path abcdaLocation, Path destination) {
    if (!abcdaLocation.toFile().exists()) {
      throw new FileNotFoundException(
          "abcdaLocation does not exist: " + abcdaLocation.toAbsolutePath());
    }

    if (destination.toFile().exists()) {
      // clean up any existing folder
      log.debug("Deleting existing archive folder [{}]", destination.toAbsolutePath());
      org.gbif.utils.file.FileUtils.deleteDirectoryRecursively(destination.toFile());
    }
    FileUtils.forceMkdir(destination.toFile());
    // try to decompress archive

    CompressionUtil.decompressFile(destination.toFile(), abcdaLocation.toFile(), false);

    return destination.toFile().listFiles((FileFilter) HiddenFileFilter.VISIBLE);
  }
}
