package org.gbif.validator.orchestrator.step;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashSet;
import java.util.Set;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;

/** Adapts a successful analyser result into the existing pipelines balancer handoff. */
public final class DwcdpStageHandoffAdapter
    implements MessageAdapter<JsonNode, PipelinesBalancerMessage> {

  private final ObjectMapper mapper;

  public DwcdpStageHandoffAdapter(ObjectMapper mapper) {
    this.mapper = java.util.Objects.requireNonNull(mapper, "mapper");
  }

  @Override
  public PipelinesBalancerMessage adapt(JsonNode completion, JobContext context)
      throws JsonProcessingException {
    if (completion == null || !completion.path("valid").asBoolean(false)) {
      throw new IllegalArgumentException("Cannot hand off unsuccessful DWc-DP analysis");
    }

    if (!context.datasetUuid().toString().equals(completion.path("datasetUuid").asText())
        || context.attempt() != completion.path("attempt").asInt(-1)) {
      throw new IllegalArgumentException("DWc-DP completion does not match workflow context");
    }

    Set<String> steps = new LinkedHashSet<>();
    for (StepType step : context.pipelineSteps()) {
      if (!step.name().startsWith("VALIDATOR_")) {
        throw new IllegalArgumentException("Unexpected ingestion step in validation: " + step);
      }
      steps.add(step.name());
    }

    if (!steps.contains(StepType.VALIDATOR_DWCDP_STAGE.name())
        || !steps.contains(StepType.VALIDATOR_DWCDP_TO_VERBATIM.name())) {
      throw new IllegalArgumentException("DWc-DP validation workflow has no staging chain");
    }

    JsonNode resources = completion.path("validationReport").path("resourceAnalysisResults");
    if (!resources.isArray() || resources.isEmpty()) {
      throw new IllegalArgumentException("DWc-DP report has no resource analysis information");
    }

    boolean occurrences = false;
    boolean events = false;
    for (JsonNode resource : resources) {
      JsonNode nameNode = resource.path("name");
      if (!nameNode.isTextual() || nameNode.asText().isBlank()) {
        throw new IllegalArgumentException("DWc-DP report contains an unnamed resource");
      }

      String name = nameNode.asText().trim();
      if ("occurrence".equalsIgnoreCase(name)) {
        occurrences = true;
      } else if ("event".equalsIgnoreCase(name)) {
        events = true;
      }
    }

    if (!occurrences && !events) {
      throw new IllegalArgumentException("DWc-DP report contains no recognized Event or Occurrence resource");
    }

    DwcDpStageMessage stage =
        new DwcDpStageMessage(
            context.datasetUuid(), context.attempt(), steps, null, occurrences, events);
    return new PipelinesBalancerMessage(
        DwcDpStageMessage.class.getSimpleName(), mapper.writeValueAsString(stage));
  }
}
