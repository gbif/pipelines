package org.gbif.validator.orchestrator.config;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.MessageListener;
import org.gbif.validator.orchestrator.service.DwcdpCompletionCallback;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the DWc-DP completion consumer when explicitly enabled.
 *
 * <p>Disabled by default until conditional status transitions and dispatch recovery are available.
 */
@Configuration
public class DwcdpCompletionListenerConfiguration {

  @Bean
  @ConditionalOnProperty(
      name = "orchestrator.unsafe-enable-completion-consumer",
      havingValue = "true")
  public ApplicationRunner dwcdpCompletionConsumer(
      MessageListener listener,
      OrchestratorProperties properties,
      DwcdpCompletionCallback callback) {

    return args -> {
      OrchestratorProperties.Completion completion =
          properties.requireStep(StepType.VALIDATOR_VALIDATE_DWCDP).getCompletion();

      validateCompletion(completion);

      listener.listen(
          completion.getQueue(),
          completion.getRoutingKey(),
          completion.getExchange(),
          completion.getThreads(),
          callback);
    };
  }

  private static void validateCompletion(OrchestratorProperties.Completion completion) {
    if (completion == null
        || isBlank(completion.getQueue())
        || isBlank(completion.getExchange())
        || isBlank(completion.getRoutingKey())
        || completion.getThreads() < 1) {
      throw new IllegalStateException("Incomplete DWc-DP completion listener configuration");
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
