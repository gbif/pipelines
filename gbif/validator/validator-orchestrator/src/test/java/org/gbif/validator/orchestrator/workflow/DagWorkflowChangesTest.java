package org.gbif.validator.orchestrator.workflow;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;
import org.junit.jupiter.api.Test;

class DagWorkflowChangesTest {

  private static final StepType ANALYSE = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType STAGE = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType CONVERT = StepType.VALIDATOR_DWCDP_TO_VERBATIM;

  @Test
  void skipProposalExcludesBranchAndSchedulesRemainingStep() {
    DagDefinition dag = new DagDefinition(
        List.of(
            new DagDefinition.Node(ANALYSE, Set.of()),
            new DagDefinition.Node(STAGE, Set.of(ANALYSE)),
            new DagDefinition.Node(CONVERT, Set.of(ANALYSE))),
        DagDefinition.JoinPolicy.NON_ERROR, false);

    WorkflowState state = new WorkflowState(Status.RUNNING, List.of(
        new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
        new WorkflowState.Step(STAGE, 1, Status.PENDING),
        new WorkflowState.Step(CONVERT, 2, Status.PENDING)));

    WorkflowDecision decision = new DagWorkflowEngine(dag).evaluate(
        state, WorkflowEvent.completed(ANALYSE), List.of(WorkflowChange.skip(STAGE)));

    assertTrue(decision.actions().stream().anyMatch(action ->
        action.kind() == WorkflowDecision.Kind.SET_STEP_STATUS
            && action.step() == STAGE && action.status() == Status.SKIPPED));
    assertTrue(decision.actions().stream().anyMatch(action ->
        action.kind() == WorkflowDecision.Kind.DISPATCH && action.step() == CONVERT));
  }

  @Test
  void cannotSkipAlreadyQueuedStep() {
    DagDefinition dag = new DagDefinition(
        List.of(new DagDefinition.Node(ANALYSE, Set.of()),
            new DagDefinition.Node(STAGE, Set.of(ANALYSE))),
        DagDefinition.JoinPolicy.NON_ERROR, false);

    WorkflowState state = new WorkflowState(Status.RUNNING, List.of(
        new WorkflowState.Step(ANALYSE, 0, Status.QUEUED),
        new WorkflowState.Step(STAGE, 1, Status.QUEUED)));

    assertThrows(IllegalArgumentException.class, () ->
        new DagWorkflowEngine(dag).evaluate(state, WorkflowEvent.completed(ANALYSE),
            List.of(WorkflowChange.skip(STAGE))));
  }
}
