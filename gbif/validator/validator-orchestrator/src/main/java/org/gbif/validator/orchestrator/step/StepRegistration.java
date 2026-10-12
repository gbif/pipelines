package org.gbif.validator.orchestrator.step;

import java.util.ArrayList;
import java.util.List;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;

/** Keeps the completion payload type paired with its hooks. */
public final class StepRegistration<C> {

  private final StepDefinition<C> definition;
  private final List<HookRegistration<C>> hooks;
  private final List<WorkflowChangeRegistration<C>> changeHooks;

  public StepRegistration(StepDefinition<C> definition, List<HookRegistration<C>> hooks) {
    this.definition = Objects.requireNonNull(definition, "definition must not be null");
    this.hooks = List.copyOf(Objects.requireNonNull(hooks, "hooks must not be null"));
    this.changeHooks = List.of();
  }

  public StepRegistration(
      StepDefinition<C> definition,
      List<HookRegistration<C>> hooks,
      List<WorkflowChangeRegistration<C>> changeHooks) {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.hooks = List.copyOf(Objects.requireNonNull(hooks, "hooks"));
    this.changeHooks = List.copyOf(Objects.requireNonNull(changeHooks, "changeHooks"));
  }

  public List<WorkflowChange> proposeChanges(
      Validation validation, Object payload, HookFailureReporter reporter) throws Exception {
    C completion = completionType().cast(payload);
    List<WorkflowChange> changes = new ArrayList<>();

    for (WorkflowChangeRegistration<C> registration : changeHooks) {
      try {
        changes.addAll(registration.hook().propose(validation, completion));
      } catch (Exception exception) {
        if (registration.required()) {
          throw exception;
        }

        reporter.report(stepType(), exception);
      }
    }

    return List.copyOf(changes);
  }

  public StepType stepType() {
    return definition.stepType();
  }

  public Class<C> completionType() {
    return definition.completionType();
  }

  public Object createRequest(Validation validation) {
    return definition.createRequest(validation);
  }

  public CompletionResult interpret(Object payload) {
    return definition.interpret(completionType().cast(payload));
  }

  public void runHooks(Validation validation, Object payload, HookFailureReporter reporter)
      throws Exception {
    C completion = completionType().cast(payload);

    for (HookRegistration<C> registration : hooks) {
      try {
        registration.hook().execute(validation, completion);
      } catch (Exception exception) {
        if (registration.required()) {
          throw exception;
        }

        reporter.report(stepType(), exception);
      }
    }
  }
}
