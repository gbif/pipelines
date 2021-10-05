package org.gbif.pipelines.crawler.metrics;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesMetricsCollectedMessage;
import org.gbif.converters.utils.XmlFilesReader;
import org.gbif.converters.utils.XmlTermExtractor;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.validator.IndexMetricsCollector;
import org.gbif.pipelines.validator.Validations;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.IndexableRules;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesIndexedMessage} is received. */
@Slf4j
public class MetricsCollectorCallback extends AbstractMessageCallback<PipelinesIndexedMessage>
    implements StepHandler<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage> {

  private static final StepType TYPE = StepType.VALIDATOR_COLLECT_METRICS;

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
        .stepType(TYPE)
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
        collectChecklistsMetrics(message);
      } else {
        log.info("Endpoint type {} is not supported!", message.getEndpointType());
        Validation validation = validationClient.get(message.getDatasetUuid());
        validation.setStatus(Status.FAILED);
        validationClient.update(validation);
      }
    };
  }

  @SneakyThrows
  private void collectChecklistsMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    if (message.getEndpointType() == EndpointType.DWC_ARCHIVE) {
      Archive archive = DwcaUtils.fromLocation(inputPath);
      if (archive.getExtension(DwcTerm.Taxon) != null
          || DwcTerm.Taxon == archive.getCore().getRowType()) {
        PipelinesChecklistValidatorMessage checklistValidatorMessage =
            new PipelinesChecklistValidatorMessage(
                message.getDatasetUuid(),
                message.getAttempt(),
                message.getPipelineSteps(),
                message.getExecutionId(),
                FileFormat.DWCA.name());
        publisher.sendAndReceive(
            checklistValidatorMessage,
            PipelinesChecklistValidatorMessage.ROUTING_KEY,
            true,
            UUID.randomUUID().toString(),
            config.checklistReplyQueue,
            response -> log.info("Response received {}", response));
      }
    }
  }

  @SneakyThrows
  private void collectMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Set<Term> coreTerms = Collections.emptySet();
    Map<Extension, Set<Term>> extensionsTerms = Collections.emptyMap();

    Long coreLineCount = null;
    String coreFileName = null;

    Map<String, Long> extLineCount = new HashMap<>();
    Map<String, String> extFiles = new HashMap<>();

    if (message.getEndpointType() == EndpointType.DWC_ARCHIVE) {
      Archive archive = DwcaUtils.fromLocation(inputPath);
      ArchiveFile core = archive.getCore();
      // Get core file name
      coreFileName = core.getLocationFile().getName();
      // Extract all terms
      coreTerms = DwcaUtils.getCoreTerms(archive);
      extensionsTerms = DwcaUtils.getExtensionsTerms(archive);
      // Count files lines
      coreLineCount = countLines(core.getLocationFile(), areHeaderLinesIncluded(core));
      for (ArchiveFile ext : archive.getExtensions()) {
        String extName = ext.getRowType().qualifiedName();
        extLineCount.put(extName, countLines(ext.getLocationFile(), areHeaderLinesIncluded(ext)));
        extFiles.put(extName, ext.getLocationFile().getName());
      }
    } else if (message.getEndpointType() == EndpointType.BIOCASE_XML_ARCHIVE) {
      List<File> files = XmlFilesReader.getInputFiles(inputPath.toFile());
      // Extract all terms
      XmlTermExtractor extractor = XmlTermExtractor.extract(files);
      coreTerms = extractor.getCore();
      extensionsTerms = extractor.getExtenstionsTerms();
    }

    // Collect metrics from ES
    Metrics metrics =
        IndexMetricsCollector.builder()
            .coreTerms(coreTerms)
            .extensionsTerms(extensionsTerms)
            .key(message.getDatasetUuid())
            .index(config.indexName)
            .corePrefix(config.corePrefix)
            .extensionsPrefix(config.extensionsPrefix)
            .esHost(config.esConfig.hosts)
            .build()
            .collect();

    // Set core file name
    setFileInfo(metrics, DwcTerm.Occurrence.qualifiedName(), coreFileName, coreLineCount);

    // Set files count values and ext file names
    extFiles.forEach(
        (key, value) -> {
          Long count = extLineCount.get(key);
          setFileInfo(metrics, key, value, count);
        });

    // Get saved metrics object and merge with the result
    Validation validation = validationClient.get(message.getDatasetUuid());
    mergeWithValidation(validation, metrics);

    // Set isIndexable
    validation.getMetrics().setIndexeable(isIndexable(validation.getMetrics()));

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  private void setFileInfo(Metrics metrics, String term, String fileName, Long count) {
    for (FileInfo fileInfo : metrics.getFileInfos()) {
      if (fileInfo.getRowType().equals(term)) {
        fileInfo.setFileName(fileName);
        fileInfo.setCount(count);
      }
    }
  }

  /** Merge the validation response received from API and collected ES metrics */
  private void mergeWithValidation(Validation validation, Metrics metrics) {
    if (validation != null && metrics != null) {
      Metrics validationMetrics = validation.getMetrics();
      if (validationMetrics == null) {
        validation.setMetrics(metrics);
      } else {
        metrics.getFileInfos().forEach(fi -> Validations.mergeFileInfo(validation, fi));
      }
    }
  }

  /** Exclude header from counter */
  private boolean areHeaderLinesIncluded(ArchiveFile arhiveFile) {
    return arhiveFile.getIgnoreHeaderLines() != null && arhiveFile.getIgnoreHeaderLines() > 0;
  }

  /** Efficient way of counting lines */
  private long countLines(File file, boolean areHeaderLinesIncluded) {
    long lines = areHeaderLinesIncluded ? -1 : 0;
    try (BufferedReader reader = Files.newBufferedReader(file.toPath(), UTF_8)) {
      while (reader.readLine() != null) {
        lines++;
      }
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
    return lines;
  }

  /** Check that all steps were completed(except current) and there is no non-indexable issue */
  private boolean isIndexable(Metrics metrics) {
    boolean finishedAllSteps =
        metrics.getStepTypes().stream()
            .filter(x -> !x.getStepType().equals(TYPE.name()))
            .noneMatch(y -> y.getStatus() != Status.FINISHED);

    boolean noNonIndexableIssue =
        metrics.getFileInfos().stream()
            .map(FileInfo::getIssues)
            .flatMap(Collection::stream)
            .map(
                x -> {
                  try {
                    return EvaluationType.valueOf(x.getIssue());
                  } catch (Exception ex) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .allMatch(IndexableRules::isIndexable);

    return finishedAllSteps && noNonIndexableIssue;
  }
}
