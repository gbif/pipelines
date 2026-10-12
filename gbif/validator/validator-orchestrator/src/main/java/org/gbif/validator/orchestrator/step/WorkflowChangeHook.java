package org.gbif.validator.orchestrator.step;

import java.util.List;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;

/** Produces declarative workflow changes; does not mutate persisted state directly. */
@FunctionalInterface
public interface WorkflowChangeHook<C> {
  List<WorkflowChange> propose(Validation validation, C completion) throws Exception;
}
