package org.gbif.pipelines.tasks.verbatims.xml;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllInterpretationAsString;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllValidatorInterpretationAsString;
import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;
import static org.gbif.pipelines.common.utils.HdfsUtils.buildOutputPath;
import static org.gbif.pipelines.common.utils.PathUtil.buildXmlInputPath;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.http.client.HttpClient;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.XmlToAvroConverter;
import org.gbif.pipelines.common.GbifApi;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Call back which is called when the {@link PipelinesXmlMessage} is received. */
@Slf4j
@Builder
public class XmlToAvroCallback extends AbstractMessageCallback<PipelinesXmlMessage>
    implements StepHandler<PipelinesXmlMessage, PipelinesVerbatimMessage> {

  public static final int SKIP_RECORDS_CHECK = -1;

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final DatasetClient datasetClient;
  private final ExecutorService executor;
  private final HttpClient httpClient;

  @Override
  public void handleMessage(PipelinesXmlMessage message) {

    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);

    StepType type = isValidator ? StepType.VALIDATOR_XML_TO_VERBATIM : StepType.XML_TO_VERBATIM;

    PipelinesCallback.<PipelinesXmlMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(type)
        .isValidator(isValidator)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    PipelinesXmlMessage message = new PipelinesXmlMessage();
    if (config.validatorOnly) {
      message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_XML_TO_VERBATIM.name()));
    }
    return message.getRoutingKey();
  }

  public Runnable createRunnable(
      UUID datasetId, String attempt, int expectedRecords, boolean isValidator) {
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

      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
      // Run main conversion process
      boolean isConverted =
          XmlToAvroConverter.create()
              .executor(executor)
              .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
              .syncInterval(config.avroConfig.syncInterval)
              .hdfsConfigs(hdfsConfigs)
              .inputPath(inputPath)
              .outputPath(outputPath)
              .metaPath(metaPath)
              .skipDeletion(isValidator)
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
    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    return createRunnable(datasetId, attempt, message.getTotalRecordCount(), isValidator);
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesXmlMessage message) {

    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(
          new HashSet<>(
              Arrays.asList(
                  StepType.XML_TO_VERBATIM.name(),
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
        message.getExecutionId(),
        DatasetType.OCCURRENCE);
  }

  @Override
  public boolean isMessageCorrect(PipelinesXmlMessage message) {
    boolean isPlatform = Platform.PIPELINES.equivalent(message.getPlatform());
    if (!isPlatform) {
      log.info("Skipping, because the platform is incorrect");
    }
    boolean isTotalCount = message.getTotalRecordCount() != 0;
    if (!isTotalCount) {
      log.info("Skipping, because total count of records is 0");
    }
    boolean isReason = message.getReason() == FinishReason.NORMAL;
    if (!isReason) {
      log.info("Skipping, because the reason is {}", message.getReason());
    }
    return isPlatform && isTotalCount && isReason;
  }

  @SneakyThrows
  private void checkRecordsSize(String datasetId, String attempt, int expectedRecords) {
    if (expectedRecords == SKIP_RECORDS_CHECK || httpClient == null) {
      return;
    }
    long currentSize = GbifApi.getIndexSize(httpClient, config.stepConfig.registry, datasetId);
    String metaFileName = new DwcaToAvroConfiguration().metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    Optional<Double> fileNumber =
        HdfsUtils.getDoubleByKey(hdfsConfigs, metaPath, Metrics.ARCHIVE_TO_ER_COUNT);

    if (!fileNumber.isPresent()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (currentSize > 0) {
      double persentage = fileNumber.get() * 100 / currentSize;
      log.info("The dataset conversion from xml to avro got {}% of records", persentage);
      if (persentage < config.failIfDropLessThanPercent) {
        throw new IllegalArgumentException(
            "Dataset - "
                + datasetId
                + " attempt - "
                + attempt
                + " the dataset conversion from xml to avro got less 80% of records");
      }
    }
  }
}
