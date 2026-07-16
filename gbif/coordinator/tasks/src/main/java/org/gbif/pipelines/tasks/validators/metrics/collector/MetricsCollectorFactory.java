package org.gbif.pipelines.tasks.validators.metrics.collector;

import lombok.Builder;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.pipelines.core.utils.DatasetTypePredicate;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class MetricsCollectorFactory {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final ValidationWsClient validationClient;
  private final PipelinesIndexedMessage message;
  private final StepType stepType;

  public MetricsCollector create() {

    // DWCA — use Spark-based collector when configured, otherwise fall back to Elasticsearch
    if (DatasetTypePredicate.isEndpointDwca(message.getEndpointType())) {
      if (config.useSparkMetrics) {
        return SparkMetricsCollector.builder()
            .config(config)
            .publisher(publisher)
            .validationClient(validationClient)
            .message(message)
            .stepType(stepType)
            .build();
      }
      return DwcaMetricsCollector.builder()
          .config(config)
          .publisher(publisher)
          .validationClient(validationClient)
          .message(message)
          .stepType(stepType)
          .build();
    }

    // XML
    if (DatasetTypePredicate.isEndpointXml(message.getEndpointType())) {
      return XmlMetricsCollector.builder().build();
    }

    // Default
    return DefaultMetricsCollector.builder()
        .validationClient(validationClient)
        .message(message)
        .build();
  }
}
