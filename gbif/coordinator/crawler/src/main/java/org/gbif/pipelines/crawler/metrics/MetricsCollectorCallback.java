package org.gbif.pipelines.crawler.metrics;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesMetricsCollectedMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.DwcaTermUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.validator.MetricsCollector;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;
import org.gbif.validator.api.Validation.Metrics;

/** Callback which is called when the {@link PipelinesIndexedMessage} is received. */
@Slf4j
public class MetricsCollectorCallback extends AbstractMessageCallback<PipelinesIndexedMessage>
    implements StepHandler<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage> {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;

  public MetricsCollectorCallback(
      MetricsCollectorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
  }

  @Override
  public void handleMessage(PipelinesIndexedMessage message) {
    PipelinesCallback.<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage>builder()
        .client(client)
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
    return true;
  }

  @Override
  public Runnable createRunnable(PipelinesIndexedMessage message) {
    return () -> {
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive = DwcaTermUtils.fromLocation(inputPath);
      Set<Term> coreTerms = DwcaTermUtils.getCoreTerms(archive);
      Map<Extension, Set<Term>> extenstionsTerms = DwcaTermUtils.getExtensionsTerms(archive);

      Metrics result =
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

      log.info(result.toString());
    };
  }
}
