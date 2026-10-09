package org.gbif.validator.orchestrator.workflow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.workflow.WorkflowState.Status;
import org.junit.jupiter.api.Test;

class SequentialWorkflowEngineTest {
  private final SequentialWorkflowEngine engine = new SequentialWorkflowEngine();
  private static final StepType A = StepType.VALIDATOR_VALIDATE_DWCDP;
  private static final StepType B = StepType.VALIDATOR_DWCDP_STAGE;
  private static final StepType C = StepType.VALIDATOR_DWCDP_TO_VERBATIM;

  private WorkflowState state(Status status, Status a, Status b, Status c) {
    // Deliberately shuffled to verify persisted execution order, not list/enum order.
    return new WorkflowState(status, List.of(
        new WorkflowState.Step(C, 30, c),
        new WorkflowState.Step(A, 10, a),
        new WorkflowState.Step(B, 20, b)));
  }

  @Test void startQueuesFirstPendingStep() {
    var result = engine.evaluate(state(Status.QUEUED, Status.PENDING, Status.PENDING, Status.PENDING), WorkflowEvent.start());
    assertEquals(List.of(WorkflowDecision.Kind.SET_STEP_STATUS,
        WorkflowDecision.Kind.SET_VALIDATION_STATUS, WorkflowDecision.Kind.DISPATCH),
        result.actions().stream().map(WorkflowDecision.Action::kind).toList());
    assertEquals(A, result.actions().get(2).step());
    assertEquals(Status.QUEUED, result.actions().get(0).status());
  }

  @Test void successQueuesFollowingStep() {
    var result = engine.evaluate(state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING), WorkflowEvent.completed(A));
    assertEquals(Status.FINISHED, result.actions().get(0).status());
    assertEquals(B, result.actions().get(1).step());
    assertEquals(B, result.actions().get(3).step());
  }

  @Test void lastStepFinishesValidation() {
    var result = engine.evaluate(state(Status.RUNNING, Status.FINISHED, Status.FINISHED, Status.RUNNING), WorkflowEvent.completed(C));
    assertEquals(2, result.actions().size());
    assertEquals(WorkflowDecision.Kind.SET_VALIDATION_STATUS, result.actions().get(1).kind());
    assertEquals(Status.FINISHED, result.actions().get(1).status());
  }

  @Test void failureStopsExecution() {
    var result = engine.evaluate(state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING), WorkflowEvent.failed(A));
    assertEquals(2, result.actions().size());
    assertEquals(Status.FAILED, result.actions().get(0).status());
    assertEquals(Status.FAILED, result.actions().get(1).status());
  }

  @Test void duplicateCompletionDoesNothing() {
    assertTrue(engine.evaluate(state(Status.RUNNING, Status.FINISHED, Status.QUEUED, Status.PENDING), WorkflowEvent.completed(A)).isNoop());
  }

  @Test void unexpectedCompletionDoesNothing() {
    assertTrue(engine.evaluate(state(Status.RUNNING, Status.RUNNING, Status.PENDING, Status.PENDING), WorkflowEvent.completed(B)).isNoop());
  }

  @Test void cancelledValidationDoesNothing() {
    assertTrue(engine.evaluate(state(Status.ABORTED, Status.RUNNING, Status.PENDING, Status.PENDING), WorkflowEvent.completed(A)).isNoop());
  }

  @Test void neverSkipsQueuedWork() {
    assertTrue(engine.evaluate(state(Status.RUNNING, Status.QUEUED, Status.PENDING, Status.PENDING), WorkflowEvent.start()).isNoop());
  }

  @Test void neverSkipsFailedWork() {
    assertTrue(engine.evaluate(state(Status.RUNNING, Status.FAILED, Status.PENDING, Status.PENDING), WorkflowEvent.start()).isNoop());
  }

  @Test void rejectsDuplicateStepOrders() {
    assertThrows(IllegalArgumentException.class, () -> new WorkflowState(Status.QUEUED, List.of(
        new WorkflowState.Step(A, 1, Status.PENDING),
        new WorkflowState.Step(B, 1, Status.PENDING))));
  }
}
