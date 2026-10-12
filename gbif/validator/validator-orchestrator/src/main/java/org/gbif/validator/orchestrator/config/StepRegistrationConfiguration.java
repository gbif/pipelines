package org.gbif.validator.orchestrator.config;

import java.util.List;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.step.DwcdpAnalysisStepDefinition;
import org.gbif.validator.orchestrator.step.DwcdpValidationReportHook;
import org.gbif.validator.orchestrator.step.DwcdpWorkerStepDefinition;
import org.gbif.validator.orchestrator.step.HookFailureReporter;
import org.gbif.validator.orchestrator.step.HookRegistration;
import org.gbif.validator.orchestrator.step.StepRegistration;
import org.gbif.validator.orchestrator.step.StepRegistry;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StepRegistrationConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StepRegistrationConfiguration.class);

  @Bean
  public StepRegistry stepRegistry(ValidationWsClient client) {
    return new StepRegistry(List.of(
        new StepRegistration<>(
            new DwcdpAnalysisStepDefinition(),
            List.of(HookRegistration.required(new DwcdpValidationReportHook(client)))),
        new StepRegistration<>(
            new DwcdpWorkerStepDefinition(StepType.VALIDATOR_DWCDP_STAGE),
            List.of()),
        new StepRegistration<>(
            new DwcdpWorkerStepDefinition(StepType.VALIDATOR_DWCDP_TO_VERBATIM),
            List.of())));
  }

  @Bean
  public HookFailureReporter hookFailureReporter() {
    return (step, exception) ->
        LOG.warn("Optional completion hook failed for {}", step, exception);
  }
}
