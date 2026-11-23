package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import org.gbif.pipelines.core.config.model.OrchestratorConfig;

/**
 * Where steps dispatched by the flow-orchestrator send their {@code StepCompleted}. Only used for
 * messages that carry a step header; other runs report to validator-ws as before.
 */
public class OrchestratorConfiguration {

  @Parameter(names = "--orchestrator-exchange")
  public String exchange = "orchestrator";

  /** The orchestrator instance's completion routing key, e.g. {@code validation.step.completed}. */
  @Parameter(names = "--orchestrator-completion-routing-key")
  public String completionRoutingKey;

  public OrchestratorConfig toConfig() {
    OrchestratorConfig config = new OrchestratorConfig();
    config.setExchange(exchange);
    config.setCompletionRoutingKey(completionRoutingKey);
    return config;
  }
}
