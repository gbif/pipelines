package org.gbif.validator.orchestrator.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.step.DwcdpAnalysisStepDefinition;
import org.gbif.validator.orchestrator.step.DwcdpStageHandoffAdapter;
import org.gbif.validator.orchestrator.step.StepRegistration;
import org.junit.jupiter.api.Test;

class StepCompletionListenerConfigurationTest {

  private final StepRegistration<?> registration =
      StepRegistration.builder(new DwcdpAnalysisStepDefinition())
          .adaptOutgoing(new DwcdpStageHandoffAdapter(new ObjectMapper()))
          .build();

  @Test
  void acceptsCompleteHandoffConfiguration() {
    assertDoesNotThrow(
        () ->
            StepCompletionListenerConfiguration.validateRegistration(
                properties(true), registration));
  }

  @Test
  void rejectsMissingHandoffDestination() {
    assertThrows(
        IllegalStateException.class,
        () ->
            StepCompletionListenerConfiguration.validateRegistration(
                properties(false), registration));
  }

  @Test
  void rejectsIncompleteRequestDestination() {
    OrchestratorProperties properties = properties(true);
    properties.requireStep(registration.stepType()).getRequest().setRoutingKey(null);

    assertThrows(
        IllegalStateException.class,
        () -> StepCompletionListenerConfiguration.validateRegistration(properties, registration));
  }

  @Test
  void rejectsInvalidCompletionThreads() {
    OrchestratorProperties properties = properties(true);
    properties.requireStep(registration.stepType()).getCompletion().setThreads(0);

    assertThrows(
        IllegalStateException.class,
        () -> StepCompletionListenerConfiguration.validateRegistration(properties, registration));
  }

  private OrchestratorProperties properties(boolean outgoing) {
    OrchestratorProperties.StepMessaging messaging = new OrchestratorProperties.StepMessaging();
    messaging.getRequest().setExchange("occurrence");
    messaging.getRequest().setRoutingKey("validator.dwcdp.analysis");
    messaging.getCompletion().setExchange("occurrence");
    messaging.getCompletion().setRoutingKey("validator.dwcdp.analysis.finished");
    messaging.getCompletion().setQueue("validator_dwcdp_analysis_finished");

    if (outgoing) {
      messaging.getOutgoing().setExchange("occurrence");
      messaging.getOutgoing().setRoutingKey("occurrence.pipelines.balancer");
    }

    OrchestratorProperties properties = new OrchestratorProperties();
    properties.setSteps(Map.of(StepType.VALIDATOR_VALIDATE_DWCDP, messaging));
    return properties;
  }
}
