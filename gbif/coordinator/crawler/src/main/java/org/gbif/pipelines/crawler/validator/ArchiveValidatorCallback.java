package org.gbif.pipelines.crawler.validator;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.net.URI;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwc.Archive;
import org.gbif.pipelines.core.utils.DwcaTermUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;
import org.gbif.validator.api.Validation.Metrics;
import org.gbif.validator.api.Validation.Metrics.ArchiveValidationReport;

/** Callback which is called when the {@link PipelinesArchiveValidatorMessage} is received. */
@Slf4j
public class ArchiveValidatorCallback
    extends AbstractMessageCallback<PipelinesArchiveValidatorMessage>
    implements StepHandler<PipelinesArchiveValidatorMessage, PipelineBasedMessage> {

  private final ArchiveValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;

  public ArchiveValidatorCallback(
      ArchiveValidatorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
  }

  @Override
  public void handleMessage(PipelinesArchiveValidatorMessage message) {
    PipelinesCallback.<PipelinesArchiveValidatorMessage, PipelineBasedMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesArchiveValidatorMessage message) {
    return true;
  }

  @Override
  public Runnable createRunnable(PipelinesArchiveValidatorMessage message) {
    return () -> {
      if (message.getEndpointType() == EndpointType.DWC_ARCHIVE) {
        Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
        Archive archive = DwcaTermUtils.fromLocation(inputPath);
        DwcaValidationReport report =
            DwcaValidator.builder()
                .archive(archive)
                .datasetKey(message.getDatasetUuid())
                .datasetType(DatasetType.OCCURRENCE)
                .maxExampleErrors(config.maxExampleErrors)
                .maxRecords(config.maxRecords)
                .build()
                .validate();

        // TODO: Get metrics result from DB and populate ArchiveValidationReport
        Metrics metrics =
            Metrics.builder()
                .archiveValidationReport(
                    ArchiveValidationReport.builder()
                        .genericReport(report.getGenericReport())
                        .occurrenceReport(report.getOccurrenceReport())
                        .invalidationReason(report.getInvalidationReason())
                        .build())
                .build();
        log.info(metrics.toString());
      }
    };
  }

  @SneakyThrows
  @Override
  public PipelineBasedMessage createOutgoingMessage(PipelinesArchiveValidatorMessage message) {
    if (message.getEndpointType() == EndpointType.DWC_ARCHIVE) {
      // TODO: ONLY FOR TESTING
      return new PipelinesDwcaMessage(
          message.getDatasetUuid(),
          DatasetType.OCCURRENCE,
          new URI("https://gbif.org"),
          message.getAttempt(),
          new DwcaValidationReport(
              message.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)),
          message.getPipelineSteps(),
          EndpointType.DWC_ARCHIVE,
          Platform.PIPELINES,
          1L,
          true);
    }
    throw new IllegalArgumentException(
        "EndpointType " + message.getEndpointType() + " is not supported");
  }
}
