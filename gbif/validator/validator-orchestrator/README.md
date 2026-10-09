# Validator orchestrator

## Purpose

Coordinates the **data-validation** workflow using the ordered validation steps persisted in `validator-ws`. The initial integration targets `VALIDATOR_VALIDATE_DWCDP`; the longer-term design also supports the crawling/ingestion domain with independently deployed orchestration and worker instances.

Worker code is domain-agnostic. RabbitMQ connection/vhost, exchange, routing keys and consumer queues are deployment configurable. The only hard isolation requirement for the first deployment is **distinct consumer queues**. There are no changes to `postal-api`.

## Architecture

- `config/OrchestratorProperties`: `StepType` → request destination and completion consumer binding.
- `config/MessagingConfiguration`: `postal-api` publisher/listener; the connection and virtual host come from `spring.rabbitmq.*`.
- `config/ValidationClientConfiguration`: uses existing `ValidationWsClient`.
- `workflow/`: pure sequential engine (`WorkflowState` + `WorkflowEvent` → `WorkflowDecision`) and mapper from persisted validation metrics.
- `service/OrchestrationService`: retrieves state, evaluates workflow, persists status changes and publishes dispatch actions. **Not production-safe yet.**
- `service/DwcdpAnalysisRequestFactory`: request payload adapter for the analyser.
- `service/DwcdpCompletionInterpreter`, `DwcdpCompletionHandler`, `DwcdpCompletionCallback`: completion JSON → workflow event, routed through the engine.
- `config/DwcdpCompletionListenerConfiguration`: optional RabbitMQ consumer registered at startup.

`StepSelector` and `ValidationWorkflowReader` are earlier read-only helpers. They are currently redundant with the engine's step selection and `ValidationWsClient` access within `OrchestrationService`; decide whether to remove them after validating the integration.

## Configuration

See `src/main/resources/application.yml` for the example. `orchestrator.steps.VALIDATOR_VALIDATE_DWCDP` configures outgoing request exchange/routing key and incoming completion exchange/routing key/queue/threads. These are **placeholder destinations**, not production RabbitMQ topology.

The completion listener is disabled by default. `ORCHESTRATOR_UNSAFE_ENABLE_COMPLETION_CONSUMER=true` enables the prototype consumer **only for controlled development**.

## Decisions

- `validator-ws` remains the workflow-state source; do not resolve the steps a second time from `FileFormat`.
- Persisted `executionOrder` determines sequencing, not enum order or config declaration order.
- Keep transport routing independent of message contracts. Use explicit `postal-api` send/listen methods.
- A completion listener identifies its step; messages need not contain a `StepType` field.
- `datasetUuid` maps to the validation UUID; attempt `1` is an initial compatibility assumption.
- For now, `valid=false` from the analyser stops the workflow. This is **provisional business policy**, not necessarily an infrastructure failure.
- Preserve readable Java formatting, explicit imports and braces for every control-flow body.

## Known limitations / choices pending

1. **Atomicity/idempotency:** Full-object `ValidationWsClient.update(validation)` is not a conditional step transition. Concurrent events may overwrite state or double-dispatch. Status update then MQ publish can leave queued-but-unsent jobs after a crash. A transactional outbox or equivalent recovery strategy is needed for production.
2. **Message failures:** Existing `postal-api` callback failures result in `basicNack(..., false, false)`; failed processing is not requeued automatically. Define a DLQ/retry policy.
3. **Completion reporting:** `validationReport` is validated for presence but its contents are not persisted into validation metrics. Decide how the report is stored and exposed by `validator-ws`.
4. **Analyser errors:** Distinguish unsuccessful execution from successfully determining that a datapackage is invalid. The current completion contract alone does not represent infrastructure failure clearly.
5. **Entry-point migration:** The supplied `validator-ws` has both `ValidationMessageService` and direct `ValidationServiceImpl.notify()` publishing logic. Cut over to one orchestrator request path, never publish the initial work twice.
6. **Other steps:** Stage, DWc-DP→verbatim, interpretation and metrics steps have no registered request adapters/listeners yet.
7. **Build integration:** This archive does not contain or modify the Maven `pom.xml`; compile/test against actual project dependencies remains unverified.

## Testing

`SequentialWorkflowEngineTest` covers core flow decisions. `DwcdpCompletionInterpreterTest` covers analyser completion parsing and event classification. Run the module's Maven test suite after integrating dependencies; these tests have not been executed against the full repository.

## Delivery

Archive paths are relative to repository root (`gbif/validator/validator-orchestrator/`). Archive increment numbers are delivery revisions and are **not** application release versions. Project release history is in `CHANGELOG.md`.
