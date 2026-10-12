package org.gbif.validator.orchestrator.workflow;

import java.util.List;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;

/** Extension point for policy-specific joins; no status updates or side effects. */
@FunctionalInterface
public interface JoinEvaluator {

  JoinDecision evaluate(DagDefinition.Node node, List<Status> predecessorStatuses);
}
