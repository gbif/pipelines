# Validator Orchestrator

Application version: **4.0.43**.

## Purpose and design

The validator orchestrator consumes a validation UUID from `validator-ws`, reads the persisted step state, evaluates workflow transitions and publishes step requests. Worker messages and completion hooks are registered by `StepType`. `postal-api` stays unchanged. RabbitMQ vhost, exchanges, routing keys and queues are deployment configuration.

The `DagWorkflowEngine` supports configurable dependencies, default `NON_ERROR` joins, custom join policies, parallel dispatch (opt-in; sequential default) and declarative step skips. The existing sequential workflow remains the default if `orchestrator.workflow.nodes` is not configured. DAG node keys must currently equal the persisted workflow step types.

## Integrations

- `DwcdpAnalysisStepDefinition` produces the analyser request and interprets the completion JSON.
- A **required** `DwcdpValidationReportHook` upserts the raw analysis report via `ValidationWsClient` before workflow advancement. The `validation-ws` server **must implement** the new endpoints; otherwise completion fails.
- Hooks may report `WorkflowChange.skip(stepType)` through `WorkflowChangeRegistration`. Required change-hook failures stop processing; optional failures are logged. Workflow changes are validated by the DAG engine. No changes are applied to active or terminal nodes.
- `InitialValidationCallback` receives `{ "datasetUuid": "...", "attempt": 1 }` and invokes the same coordinator used by completion handling.

## External contracts to implement

- `gbif-api` is unchanged. Report type identifiers are local orchestrator string constants (`ValidationReportTypes`).
- `validator-ws-client`: PUT/GET `/{key}/validationreport/{attempt}/{reportType}`; server implementation is **not included**. This is an additive endpoint proposal, not verified against the existing controller.
- The shared validation status model must eventually support `SKIPPED` before DAG branch skips can persist; this version throws rather than silently translating it to another status.

## Safety limits

No automatic workflow retries are implemented. **Note:** the existing `ValidationWsClient.getInstance` still configures transport-level exponential-backoff retries; remove these separately if the no-retry policy also covers HTTP retries.

`GET`/full-object `PUT` + RabbitMQ send are not atomic. Only enable listeners for controlled, single-instance development (`orchestrator.unsafe-enable-start-consumer` and `orchestrator.unsafe-enable-completion-consumer`). Callback exceptions are rejected without requeue by `postal-api`. That does not itself ensure the validation is marked failed. No claims of production-safe dispatch or exactly-once processing.

Report persistence is an upsert to support duplicate completion attempts. Optional hooks should be idempotent to avoid repeated side effects. No automatic retries.

## Build and tests

Run Maven tests in the parent repository with the updated string-based ws-client contract. This archive does not contain a complete multi-module Maven build and has not been compiled against that repository.

## v12 integration reconciliation

The initial validation request is consumed by the orchestrator when the development-only start consumer is enabled. The existing validation UUID identifies the authoritative workflow stored by `validator-ws`. The initial DWc-DP adapter publishes the analyser request with the configurable exchange and routing key.

The DWc-DP completion processor checks that its step is still active **before** running hooks. It uploads the analyser report via `ValidationWsClient.createOrUpdateValidationReport` using the string report type `DWCDP_ANALYSIS`, then invokes the generic coordinator. The report endpoint must exist in `validator-ws`; it is not implemented by this archive. An invalid-data result is currently considered a failed workflow, after saving its report.

`DwcdpExecutionFlowTest` exercises initial dispatch, successful completion, duplicate completion, report-upload failure and invalid-data completion using an in-memory WS implementation and a recording message publisher. It is not a RabbitMQ/HTTP integration test.

**Safety:** The consumer flags remain disabled by default. The existing `GET`/full-object `PUT` plus RabbitMQ publish are not atomic; single-instance processing alone does not eliminate crash windows. There are no automatic workflow retries; malformed/rejected RabbitMQ deliveries still require operational handling. The status `SKIPPED` still requires validator-api/validator-ws support before a live DAG skip can be persisted.

## Thin validation worker adapters

`VALIDATOR_DWCDP_STAGE`, `VALIDATOR_DWCDP_TO_VERBATIM`, and
`VALIDATOR_VERBATIM_TO_IDENTIFIER` use a proposed thin wrapper contract:
request JSON `{ "datasetUuid": "UUID", "attempt": 1 }` and completion JSON
`{ "datasetUuid": "UUID", "attempt": 1, "successful": true }`.
A `successful: false` completion is interpreted as worker failure. The wrappers
must adopt these fields (or their adapters must be changed to the actual message
contracts) before enabling consumers. This contract is **not verified** against
stage, to-verbatim, or identifier services. Routing values in `application.yml`
are illustrative. No additional completion hook is registered for these steps.

## Completion listener registration

When `orchestrator.unsafe-enable-completion-consumer=true`, one typed completion listener is registered for every `StepRegistry` entry using its configured queue, exchange, routing key and thread count. Missing destinations fail startup before listeners are created. The DWc-DP handler no longer needs dedicated listener wiring. The flag stays disabled by default: WS updates and RabbitMQ dispatch are not atomic.
