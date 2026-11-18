package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Where validator steps dispatched by the flow-orchestrator send their {@code StepCompleted}. Only
 * used for messages that carry a step header; other runs report to validator-ws as before.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrchestratorConfig implements Serializable {

  /** The orchestrator's exchange. */
  public String exchange = "orchestrator";

  /**
   * The orchestrator instance's completion routing key, for example {@code
   * validation.step.completed}.
   */
  public String completionRoutingKey;
}
