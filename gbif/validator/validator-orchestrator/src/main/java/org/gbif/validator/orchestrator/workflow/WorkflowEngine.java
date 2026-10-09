package org.gbif.validator.orchestrator.workflow;

public interface WorkflowEngine {
  WorkflowDecision evaluate(WorkflowState state, WorkflowEvent event);
}
