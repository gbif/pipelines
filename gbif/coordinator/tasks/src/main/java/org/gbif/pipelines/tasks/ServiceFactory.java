package org.gbif.pipelines.tasks;

import java.time.Duration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ServiceFactory {

  public static PipelinesHistoryClient createPipelinesHistoryClient(StepConfiguration stepConfig) {
    return new ClientBuilder()
        .withUrl(stepConfig.registry.wsUrl)
        .withCredentials(stepConfig.registry.user, stepConfig.registry.password)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .withFormEncoder()
        .build(PipelinesHistoryClient.class);
  }

  public static ValidationWsClient createValidationWsClient(StepConfiguration stepConfig) {
    return new ClientBuilder()
        .withUrl(stepConfig.registry.wsUrl)
        .withCredentials(stepConfig.registry.user, stepConfig.registry.password)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(ValidationWsClient.class);
  }

  public static DatasetClient createDatasetClient(StepConfiguration stepConfig) {
    return new ClientBuilder()
        .withUrl(stepConfig.registry.wsUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(DatasetClient.class);
  }
}
