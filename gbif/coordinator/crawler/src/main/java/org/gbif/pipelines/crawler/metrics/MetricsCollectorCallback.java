package org.gbif.pipelines.crawler.metrics;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesMetricsCollectedMessage;
import org.gbif.converters.utils.XmlFilesReader;
import org.gbif.converters.utils.XmlTermExtractor;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.validator.MetricsCollector;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesIndexedMessage} is received. */
@Slf4j
public class MetricsCollectorCallback extends AbstractMessageCallback<PipelinesIndexedMessage>
    implements StepHandler<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage> {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;

  public MetricsCollectorCallback(
      MetricsCollectorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ValidationWsClient validationClient) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.historyClient = historyClient;
    this.validationClient = validationClient;
  }

  @Override
  public void handleMessage(PipelinesIndexedMessage message) {
    PipelinesCallback.<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .curator(curator)
        .stepType(StepType.VALIDATOR_COLLECT_METRICS)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesIndexedMessage message) {
    return message.getDatasetUuid() != null && message.getEndpointType() != null;
  }

  @Override
  public Runnable createRunnable(PipelinesIndexedMessage message) {
    return () -> {
      log.info("Running metrics collector for {}", message.getDatasetUuid());
      if (message.getEndpointType() == EndpointType.DWC_ARCHIVE
          || message.getEndpointType() == EndpointType.BIOCASE_XML_ARCHIVE) {
        log.info("Collect {} metrics for {}", message.getEndpointType(), message.getDatasetUuid());
        collectMetrics(message);
      } else {
        log.info("Endpoint type {} is not supported!", message.getEndpointType());
        Validation validation = validationClient.get(message.getDatasetUuid());
        validation.setStatus(Status.FAILED);
        validationClient.update(validation);
      }
    };
  }

  @SneakyThrows
  private void collectMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Set<Term> coreTerms = Collections.emptySet();
    Map<Extension, Set<Term>> extenstionsTerms = Collections.emptyMap();

    if (message.getEndpointType() == EndpointType.DWC_ARCHIVE) {
      Archive archive = DwcaUtils.fromLocation(inputPath);
      coreTerms = DwcaUtils.getCoreTerms(archive);
      extenstionsTerms = DwcaUtils.getExtensionsTerms(archive);
    } else if (message.getEndpointType() == EndpointType.BIOCASE_XML_ARCHIVE) {
      List<File> files = XmlFilesReader.getInputFiles(inputPath.toFile());
      XmlTermExtractor extractor = XmlTermExtractor.extract(files);
      coreTerms = extractor.getCore();
      extenstionsTerms = extractor.getExtenstionsTerms();
    }

    Metrics metrics =
        MetricsCollector.builder()
            .coreTerms(coreTerms)
            .extensionsTerms(extenstionsTerms)
            .key(message.getDatasetUuid())
            .index(config.indexName)
            .corePrefix(config.corePrefix)
            .extensionsPrefix(config.extensionsPrefix)
            .esHost(config.esConfig.hosts)
            .build()
            .collect();

    Validation validation = validationClient.get(message.getDatasetUuid());
    merge(validation, metrics);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  private void merge(Validation validation, Metrics metrics) {
    if (validation != null && metrics != null) {
      Metrics validationMetrics = validation.getMetrics();
      if (validationMetrics == null) {
        validation.setMetrics(metrics);
      } else {
        validationMetrics.setCore(metrics.getCore());
        validationMetrics.setExtensions(metrics.getExtensions());
      }
    }
  }
}
