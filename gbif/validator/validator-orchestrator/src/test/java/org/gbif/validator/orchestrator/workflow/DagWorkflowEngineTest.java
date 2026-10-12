package org.gbif.validator.orchestrator.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.DagDefinition.JoinPolicy;
import org.gbif.validator.orchestrator.workflow.DagDefinition.Node;
import org.gbif.validator.orchestrator.workflow.WorkflowDecision.Kind;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Step;
import org.junit.jupiter.api.Test;

class DagWorkflowEngineTest {

  private static final StepType ROOT = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType LEFT = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType RIGHT = StepType.VALIDATOR_DWCDP_TO_VERBATIM;
  private static final StepType JOIN = StepType.VALIDATOR_COLLECT_METRICS;

  private DagDefinition definition(boolean parallel) {
    return new DagDefinition(List.of(
        new Node(ROOT, Set.of()),
        new Node(LEFT, Set.of(ROOT)),
        new Node(RIGHT, Set.of(ROOT)),
        new Node(JOIN, Set.of(LEFT, RIGHT))), JoinPolicy.NON_ERROR, parallel);
  }

  private WorkflowState state(Status root, Status left, Status right, Status join) {
    return new WorkflowState(Status.RUNNING, List.of(
        new Step(ROOT, 0, root),
        new Step(LEFT, 1, left),
        new Step(RIGHT, 2, right),
        new Step(JOIN, 3, join)));
  }

  private List<StepType> dispatched(WorkflowDecision decision) {
    return decision.actions().stream().filter(a -> a.kind() == Kind.DISPATCH)
        .map(WorkflowDecision.Action::step).toList();
  }

  @Test
  void sequentialDispatchesOnlyOneBranch() {
    WorkflowDecision result = new DagWorkflowEngine(definition(false)).evaluate(
        state(Status.QUEUED, Status.PENDING, Status.PENDING, Status.PENDING),
        WorkflowEvent.completed(ROOT));

    assertEquals(List.of(LEFT), dispatched(result));
  }

  @Test
  void parallelDispatchesBothBranches() {
    WorkflowDecision result = new DagWorkflowEngine(definition(true)).evaluate(
        state(Status.QUEUED, Status.PENDING, Status.PENDING, Status.PENDING),
        WorkflowEvent.completed(ROOT));

    assertEquals(List.of(LEFT, RIGHT), dispatched(result));
  }

  @Test
  void sequentialDoesNotDispatchWhileAnotherBranchRuns() {
    WorkflowDecision result = new DagWorkflowEngine(definition(false)).evaluate(
        state(Status.FINISHED, Status.RUNNING, Status.PENDING, Status.PENDING),
        WorkflowEvent.start());

    assertTrue(dispatched(result).isEmpty());
  }

  @Test
  void joinWaitsForBothPredecessors() {
    WorkflowDecision result = new DagWorkflowEngine(definition(true)).evaluate(
        state(Status.FINISHED, Status.QUEUED, Status.RUNNING, Status.PENDING),
        WorkflowEvent.completed(LEFT));

    assertTrue(dispatched(result).isEmpty());
  }

  @Test
  void joinAcceptsSkippedPredecessor() {
    WorkflowDecision result = new DagWorkflowEngine(definition(true)).evaluate(
        state(Status.FINISHED, Status.QUEUED, Status.SKIPPED, Status.PENDING),
        WorkflowEvent.completed(LEFT));

    assertEquals(List.of(JOIN), dispatched(result));
  }

  @Test
  void failedPredecessorAbortsJoinAfterOtherBranchTerminates() {
    WorkflowDecision result = new DagWorkflowEngine(definition(true)).evaluate(
        state(Status.FINISHED, Status.FAILED, Status.QUEUED, Status.PENDING),
        WorkflowEvent.completed(RIGHT));

    assertTrue(dispatched(result).isEmpty());
    assertTrue(result.actions().stream().anyMatch(a -> a.kind() == Kind.SET_STEP_STATUS
        && a.step() == JOIN && a.status() == Status.ABORTED));
    assertTrue(result.actions().stream().anyMatch(a -> a.kind() == Kind.SET_VALIDATION_STATUS
        && a.status() == Status.FAILED));
  }

  @Test
  void customJoinPolicyCanAllowAbortedPredecessor() {
    DagWorkflowEngine engine = new DagWorkflowEngine(definition(true), Map.of(
        JoinPolicy.NON_ERROR, (node, statuses) -> statuses.stream().allMatch(
            status -> status == Status.FINISHED || status == Status.ABORTED)
            ? JoinDecision.EXECUTE : JoinDecision.WAIT));

    WorkflowDecision result = engine.evaluate(
        state(Status.FINISHED, Status.ABORTED, Status.QUEUED, Status.PENDING),
        WorkflowEvent.completed(RIGHT));

    assertEquals(List.of(JOIN), dispatched(result));
  }

  @Test
  void duplicateCompletionDoesNothing() {
    WorkflowDecision result = new DagWorkflowEngine(definition(true)).evaluate(
        state(Status.FINISHED, Status.FINISHED, Status.QUEUED, Status.PENDING),
        WorkflowEvent.completed(LEFT));

    assertTrue(result.isNoop());
  }

  @Test
  void rejectsCyclesAndUnknownDependencies() {
    assertThrows(IllegalArgumentException.class, () -> new DagDefinition(List.of(
        new Node(ROOT, Set.of(LEFT)), new Node(LEFT, Set.of(ROOT))),
        JoinPolicy.NON_ERROR, false));

    assertThrows(IllegalArgumentException.class, () -> new DagDefinition(List.of(
        new Node(ROOT, Set.of(LEFT))), JoinPolicy.NON_ERROR, false));
  }
}
