package org.gbif.validator.orchestrator.step;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.workflow.WorkflowChange;

/** Keeps the completion payload type paired with its hooks. */
public final class StepRegistration<C> {

  private final StepDefinition<C> definition;
  private final MessageAdapter<Object, Object> before;
  private final MessageAdapter<C, ?> after;
  private final List<HookRegistration<C>> hooks;
  private final List<WorkflowChangeRegistration<C>> changeHooks;

  public StepRegistration(StepDefinition<C> definition, List<HookRegistration<C>> hooks) {
    this.definition = Objects.requireNonNull(definition, "definition must not be null");
    this.before = null;
    this.after = null;
    this.hooks = List.copyOf(Objects.requireNonNull(hooks, "hooks must not be null"));
    this.changeHooks = List.of();
  }

  public StepRegistration(
      StepDefinition<C> definition,
      List<HookRegistration<C>> hooks,
      List<WorkflowChangeRegistration<C>> changeHooks) {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.before = null;
    this.after = null;
    this.hooks = List.copyOf(Objects.requireNonNull(hooks, "hooks"));
    this.changeHooks = List.copyOf(Objects.requireNonNull(changeHooks, "changeHooks"));
  }

  private StepRegistration(Builder<C> builder) {
    this.definition = builder.definition;
    this.hooks = List.copyOf(builder.hooks);
    this.changeHooks = List.copyOf(builder.changeHooks);
    this.before = builder.before;
    this.after = builder.after;
  }

  public static <C> Builder<C> builder(StepDefinition<C> definition) {
    return new Builder<>(definition);
  }

  public boolean hasOutgoingAdapter() {
    return after != null;
  }

  public Object adaptOutgoing(Object completion, JobContext context) throws Exception {
    C typed = completionType().cast(completion);
    return after == null ? typed : after.adapt(typed, context);
  }

  public Object adaptRequest(Object request, JobContext context) throws Exception {
    return before == null ? request : before.adapt(request, context);
  }

  public static final class Builder<C> {
    private final StepDefinition<C> definition;
    private final List<HookRegistration<C>> hooks = new ArrayList<>();
    private final List<WorkflowChangeRegistration<C>> changeHooks = new ArrayList<>();
    private MessageAdapter<Object, Object> before;
    private MessageAdapter<C, ?> after;

    private Builder(StepDefinition<C> definition) {
      this.definition = Objects.requireNonNull(definition, "definition");
    }

    public Builder<C> before(MessageAdapter<Object, Object> adapter) {
      this.before = Objects.requireNonNull(adapter, "adapter");
      return this;
    }

    public Builder<C> onJobProcessed(HookRegistration<C> hook) {
      hooks.add(Objects.requireNonNull(hook, "hook"));
      return this;
    }

    public Builder<C> onWorkflowChange(WorkflowChangeRegistration<C> hook) {
      changeHooks.add(Objects.requireNonNull(hook, "hook"));
      return this;
    }

    public Builder<C> adaptOutgoing(MessageAdapter<C, ?> adapter) {
      this.after = Objects.requireNonNull(adapter, "adapter");
      return this;
    }

    public StepRegistration<C> build() {
      return new StepRegistration<>(this);
    }
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
