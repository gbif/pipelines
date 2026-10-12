package org.gbif.validator.orchestrator.config;

import org.gbif.validator.orchestrator.service.OrchestrationService;
import org.gbif.validator.orchestrator.step.CompletionProcessor;
import org.gbif.validator.orchestrator.step.HookFailureReporter;
import org.gbif.validator.orchestrator.step.StepRegistry;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CompletionProcessorConfiguration {

  @Bean
  public CompletionProcessor completionProcessor(StepRegistry registry,
      ValidationWsClient client, OrchestrationService coordinator,
      HookFailureReporter reporter) {
    return new CompletionProcessor(registry, client, coordinator, reporter);
  }
}
