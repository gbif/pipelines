
# Validator Orchestrator

Application version: **4.0.48-SNAPSHOT**

The Validator Orchestrator coordinates validation workflows using persisted
workflow state, RabbitMQ messaging, and pluggable step definitions.

It is designed to support both validation and ingestion workflows while keeping
the workflow engine independent of individual processing services.

**Current scope:** Phase 1 implements the `VALIDATOR_VALIDATE_DWCDP` step for
Darwin Core Data Package (DwC-DP) validation. The integration has been tested
end-to-end using the real DwC-DP analyser and a mock `validator-ws`.

## 1. Architecture

The orchestrator acts as a coordinator between `validator-ws`, RabbitMQ, and
processing services.

It does not perform validation itself.

```text
                         +---------------------+
                         |    validator-ws     |
                         |                     |
                         | Persisted workflow  |
                         | Validation status   |
                         | Validation reports  |
                         +----------^----------+
                                    |
                               HTTP GET/PUT
                                    |
                                    |
+-------------------+    +----------+-------------------------+
| Initial request   |    |                                    |
|                   |    |       Validator Orchestrator       |
| datasetUuid       +--->|                                    |
| attempt           |    |  +------------------------------+  |
+-------------------+    |  | OrchestrationService         |  |
                         |  |                              |  |
                         |  | - Load persisted workflow    |  |
                         |  | - Evaluate transitions      |  |
                         |  | - Dispatch ready steps      |  |
                         |  | - Persist updated state     |  |
                         |  +---------------+--------------+  |
                         |                  |                 |
                         |  +---------------v--------------+  |
                         |  | DagWorkflowEngine            |  |
                         |  | StepRegistry                 |  |
                         |  | StepDispatcher               |  |
                         |  | Hooks and workflow changes   |  |
                         |  +---------------+--------------+  |
                         +------------------+-----------------+
                                            |
                                     RabbitMQ publish
                                            |
                                            v
                               +-------------------------+
                               |   Processing service    |
                               |                         |
                               |    DwC-DP analyser      |
                               |                         |
                               +------------+------------+
                                            |
                                   Completion message
                                            |
                                            v
                               +-------------------------+
                               |   RabbitMQ completion   |
                               |         listener        |
                               +------------+------------+
                                            |
                                            +----> Orchestrator
```

### Design principles

- Workflow state is persisted in `validator-ws`, not held as authoritative
  in-memory state by the orchestrator.
- Processing steps are integrated through registered `StepDefinition` implementations.
- Workflow evaluation is separate from RabbitMQ transport and HTTP communication.
- Worker completion messages drive subsequent workflow transitions.
- Hooks perform integration-specific side effects without coupling them to the
  generic workflow engine.
- RabbitMQ destinations are configurable for different deployments.
- No automatic workflow retries are performed.
- Sequential execution is the default; parallel execution must be explicitly
  configured.

The orchestrator uses GBIF's existing `postal-api` messaging library without
modifying it.

## 2. Workflow lifecycle

A workflow begins with a RabbitMQ request containing a validation identifier
and execution attempt.

Example:

```json
{
  "datasetUuid": "673f2308-3501-474f-85d5-a832825bb46c",
  "attempt": 1
}
```

The identifier references an existing validation record in `validator-ws`.

The orchestrator does not create that record.

### Initial request

```text
RabbitMQ initial request
         |
         v
InitialValidationCallback
         |
         v
OrchestrationService
         |
         +-- GET validation from validator-ws
         |
         +-- ValidationWorkflowMapper
         |      |
         |      +-- Read persisted workflow steps
         |      +-- Reconstruct workflow state
         |
         +-- DagWorkflowEngine
         |      |
         |      +-- Evaluate dependencies
         |      +-- Determine ready steps
         |
         +-- StepDispatcher
         |      |
         |      +-- Resolve StepDefinition
         |      +-- Construct worker request
         |      +-- Publish to RabbitMQ
         |
         +-- Persist updated workflow state
```

The initial message contains only the information required to locate the
persisted workflow and identify its execution attempt.

### Completion handling

When a worker finishes, it publishes a typed completion message.

```text
Processing service
         |
         | Completion message
         v
RabbitMQ
         |
         v
Typed completion callback
         |
         v
StepDefinition
         |
         +-- Extract dataset UUID and attempt
         +-- Interpret completion outcome
         |
         v
OrchestrationService
         |
         +-- Load persisted workflow
         |
         +-- Verify the step is active
         |
         +-- Execute completion hooks
         |
         +-- Update step outcome
         |
         +-- Evaluate next workflow transitions
         |
         +-- Dispatch newly ready steps, if any
         |
         +-- Persist workflow state
         |
         v
Workflow continues or reaches a terminal state
```

The completion callback and initial callback use the same orchestration
coordinator.

Duplicate or stale completions are checked against the persisted step state
before completion hooks are executed.

## 3. Workflow engine

The workflow engine is implemented by `DagWorkflowEngine`.

It supports dependency-based step scheduling, including workflows that are
more complex than a fixed linear pipeline.

### Sequential execution

Sequential execution remains the default.

```text
       Step A
          |
          v
       Step B
          |
          v
       Step C
          |
          v
      Completed
```

When `orchestrator.workflow.nodes` is not configured, the existing sequential
workflow structure is used.

### DAG execution

Explicit dependencies allow branching and joining:

```text
                         Step A
                            |
                   +--------+--------+
                   |                 |
                   v                 v
                 Step B            Step C
                   |                 |
                   +--------+--------+
                            |
                            v
                          Step D
                            |
                            v
                         Completed
```

Parallel dispatch is opt-in.

A step becomes eligible for execution when its dependency conditions are
satisfied according to its configured join policy.

### Join policies

The default join policy is `NON_ERROR`.

This permits dependencies that have completed without error, including
successfully finished and skipped steps.

Custom join policies can override this behavior.

The engine supports declarative skips, but live persistence of `SKIPPED`
requires corresponding support in the shared validation status model.

### Workflow changes

Hooks may request workflow changes through `WorkflowChangeRegistration`.

For example:

```text
Completion received
        |
        v
Completion hook
        |
        +-- Report outcome
        |
        +-- Request skip of another step
        |
        v
WorkflowChange
        |
        v
DagWorkflowEngine
        |
        +-- Validate requested change
        +-- Reject changes to active or terminal steps
        +-- Apply permitted changes
```

Required change-hook failures stop processing.

Optional hook failures are logged without automatically failing the workflow.

Workflow changes remain subject to engine validation.

**Current limitation:** DAG node keys must match the persisted workflow
step-type identifiers.

## 4. Step definitions

A step definition supplies the integration-specific behavior required to
execute and complete one workflow step.

The generic orchestrator does not need to know each worker's request and
completion format.

```text
                         StepRegistry
                              |
                +-------------+-------------+
                |             |             |
                v             v             v
         Definition A   Definition B   Definition C
                |             |             |
                v             v             v
           Worker A       Worker B       Worker C
```

Step definitions are registered by `StepType`.

Their responsibilities include:

- Constructing worker request messages.
- Identifying message destinations.
- Interpreting worker completion messages.
- Extracting execution identifiers and outcomes.
- Supplying associated completion hooks.

This allows different worker services to use different message contracts
while sharing the same workflow engine.

### Current implementation

Phase 1 registers:

```text
VALIDATOR_VALIDATE_DWCDP
```

Its implementation is `DwcdpAnalysisStepDefinition`.

The definition constructs the analyser request and interprets
`DwcDpValidationFinished` completion messages.

Other validation steps are not yet owned by the orchestrator.

## 5. DwC-DP validation integration

The first completed integration uses the real
`dwc-dp-analyser-service`.

### Request and completion flow

```text
                 Initial validation request
                            |
                            v
                  Validator Orchestrator
                            |
                            | DwC-DP analysis request
                            v
                         RabbitMQ
                            |
                            v
                    DWc-DP Analyser
                            |
                            +-- Resolve datapackage
                            |
                            +-- Validate datapackage
                            |
                            +-- Generate analysis report
                            |
                            v
                  DwcDpValidationFinished
                            |
                            v
                         RabbitMQ
                            |
                            v
                  Validator Orchestrator
                            |
                            +-- Read current validation
                            |
                            +-- Check active workflow step
                            |
                            +-- Upload analysis report
                            |
                            +-- Persist step outcome
                            |
                            v
                        validator-ws
```

### Validation report handling

`DwcdpValidationReportHook` uploads the analysis report using
`ValidationWsClient`.

The report type is represented by the local string constant:

```text
DWCDP_ANALYSIS
```

Report type identifiers are defined locally in `ValidationReportTypes`.
They do not require a new enum in `gbif-api`.

The report is uploaded before the workflow advances.

The corresponding API contract is:

```text
PUT /validation/{key}/validationreport/{attempt}/{reportType}
GET /validation/{key}/validationreport/{attempt}/{reportType}
```

These client operations require matching server-side endpoints in
`validator-ws`.

The local integration test uses a mock implementation of these endpoints.
Production server compatibility remains to be verified.

The upload is implemented as an upsert to support repeated completion
processing.

### Validation outcomes

The analyser returns both its execution identifiers and the validity result.

```text
             DwcDpValidationFinished
                        |
                +-------+-------+
                |               |
           valid=true       valid=false
                |               |
                v               v
             FINISHED         FAILED
                |               |
                +-------+-------+
                        |
                        v
                Upload report
                        |
                        v
                Persist outcome
```

The actual report upload occurs before the final workflow state is persisted.

An invalid datapackage is a valid processing outcome from the analyser, but
it results in a failed validation workflow.

A failed validation can still have a successfully generated and uploaded
analysis report.

## 6. Phase 1 ownership boundary

The current integration deliberately limits orchestration ownership.

```text
                 validator-ws
                      |
                      v
              Validation started
                      |
                      v
            Validator Orchestrator
                      |
                      v
          VALIDATOR_VALIDATE_DWCDP
                      |
                      v
                DwC-DP analyser
                      |
                      v
               Completion + report
                      |
                      v
            Validator Orchestrator
                      |
                +-----+-----+
                |           |
                v           v
             Valid        Invalid
                |           |
                v           v
             FINISHED      FAILED

              [Phase 1 boundary]
                       |
                       v
             Remaining validation steps
                  NOT YET WIRED
```

The following existing pipeline steps are not dispatched by this integration:

- `VALIDATOR_DWCDP_STAGE`
- `VALIDATOR_DWCDP_TO_VERBATIM`
- `VALIDATOR_VERBATIM_TO_IDENTIFIER`
- `VALIDATOR_VERBATIM_TO_INTERPRETED`
- `VALIDATOR_COLLECT_METRICS`

Their request and completion contracts must be integrated deliberately
rather than guessed from existing step names.

A handoff to the legacy validation pipeline has not yet been implemented.

Consequently, a multi-step validation cannot currently be assumed to
progress beyond the initial DwC-DP analysis through the new orchestrator.

## 7. Messaging architecture

RabbitMQ communication uses GBIF's `postal-api`.

The orchestrator creates a message publisher and message listeners through
its messaging configuration.

There are two primary inbound message categories:

1. Initial workflow requests.
2. Worker completion messages.

Outbound messages are constructed by step definitions and published to
configured exchanges and routing keys.

### Message routing

```text
                         RabbitMQ
                            |
              +-------------+--------------+
              |                            |
              v                            v
      Initial request queue        Completion queues
              |                            |
              v                            v
    InitialValidationCallback      Typed callbacks
              |                            |
              +-------------+--------------+
                            |
                            v
                  OrchestrationService
                            |
                            v
                     StepDispatcher
                            |
                            v
                     RabbitMQ publish
                            |
                            v
                       Worker queue
```

Each registered step can have its own completion queue, exchange, routing
key, and consumer thread count.

Enabled completion listeners are registered from `StepRegistry`.

The orchestrator's listeners declare their durable queues and topic
bindings through `MessageListener`.

The orchestrator does not provision the request queues of external workers.
Those queues must be declared and bound by the worker or by deployment
infrastructure.

### Deployment separation

The architecture supports separate validation and ingestion deployments.

```text
                Shared orchestration code
                           |
               +-----------+-----------+
               |                       |
               v                       v
        Validation instance      Ingestion instance
               |                       |
               v                       v
        Validation routing       Ingestion routing
               |                       |
               v                       v
        Validation queues        Ingestion queues
```

Each deployment can configure its RabbitMQ vhost, exchanges, routing keys,
and queues independently.

Queue and routing separation prevents the two deployment domains from
consuming each other's workflow messages.

Ingestion orchestration is an architectural extension point, not an
end-to-end integration verified by the current Phase 1 test.

## 8. State management and reliability

The authoritative workflow state resides in `validator-ws`.

The orchestrator reads that state, computes changes, and writes updates
through `ValidationWsClient`.

### Persistence model

```text
RabbitMQ message
       |
       v
Read workflow state
       |
       v
Evaluate / mutate workflow
       |
       +-- External side effects
       |      |
       |      +-- Publish worker messages
       |      +-- Upload reports
       |
       v
Persist updated workflow
```

These operations are not one atomic transaction.

In particular:

```text
HTTP GET
   +
Workflow evaluation
   +
HTTP PUT
   +
RabbitMQ publish
```

cannot currently be committed atomically.

This creates potential crash windows, including dispatch succeeding while
the corresponding state update fails.

### Consumer safety

The following development flags control listener activation:

```properties
orchestrator.unsafe-enable-start-consumer
orchestrator.unsafe-enable-completion-consumer
```

Both are disabled by default.

They must be explicitly enabled in the local integration environment.

These flags are intentionally named `unsafe` because the current
read/modify/write plus messaging lifecycle does not guarantee atomic
dispatch or exactly-once processing.

Running a single orchestrator instance reduces concurrency risks but
does not eliminate crash windows.

### Retry policy

Automatic workflow retries are not implemented.

A failed workflow is expected to be restarted through a new execution
rather than by silently replaying individual steps.

This policy is separate from infrastructure reconnection.

Known limitations:

- Initial RabbitMQ connection failures can prevent application startup.
- RabbitMQ Java-client recovery does not solve every initial connection failure.
- Callback exceptions are rejected without requeue by `postal-api`.
- A rejected message does not necessarily cause the persisted workflow to
  transition to `FAILED`.
- Duplicate completion handling and side-effect idempotency remain important.
- The WS client may perform transport-level exponential-backoff retries.
  These are separate from workflow retries.

Production readiness requires further examination of failure recovery,
concurrent updates, and message acknowledgement semantics.

## 9. Local integration environment

A local Compose environment is provided in `local-test/`.

It uses:

- RabbitMQ.
- The real Validator Orchestrator.
- The real DwC-DP analyser.
- A Python mock of `validator-ws`.
- A trigger script to seed and submit validation requests.

### Local topology

```text
                       Host machine
                            |
                      trigger.py
                            |
                  +---------+---------+
                  |                   |
                  v                   v
           Mock validator-ws       RabbitMQ
                  ^                   |
                  |                   v
                  |             Orchestrator
                  |                   |
                  |                   v
                  |                RabbitMQ
                  |                   |
                  |                   v
                  |              DwC-DP Analyser
                  |                   |
                  |                   v
                  |                RabbitMQ
                  |                   |
                  +-------------------+
                            |
                            v
                Workflow status and report
```

### Tested analyser image

```text
docker.gbif.org/dwc-dp-analyser-service:0.0.7-SNAPSHOT
```

The analyser's Registry integration is disabled when its Registry URL is
empty. Report persistence for this flow is handled by the orchestrator.

The analyser and orchestrator must use compatible request queues,
completion exchanges, and routing keys.


### Datapackage layout

The analyser receives its archive repository through a volume mounted at:

`/data/datapackages`

`DatapackagePathResolver` supports the following layouts, checked in order:

1. `{UUID}/{ATTEMPT}/datapackage.json`
2. `{UUID}/{UUID}.{ATTEMPT}/datapackage.json`
3. `{UUID}/datapackage.json`
4. `{UUID}/{UUID}.{ATTEMPT}/{UUID}.{ATTEMPT}.dwcdp`
5. `{UUID}/{UUID}.{ATTEMPT}/{UUID}.{ATTEMPT}.zip`

For example:

```text
/data/datapackages/
└── <uuid>/
    ├── <attempt>/
    │   ├── datapackage.json
    │   └── data/
    ├── <uuid>.<attempt>/
    │   ├── datapackage.json
    │   └── data/
    └── datapackage.json
```

These are alternative layouts; only one matching datapackage is required.

UUID-containing filesystem paths should use lowercase hexadecimal UUIDs,
matching Java's `UUID.toString()` representation.

### Environment configuration

The local Compose environment uses host-side variables such as:

```dotenv
DATAPACKAGES_DIR=/absolute/path/to/datapackages
DWCDP_WORKDIR=/absolute/path/to/workdir
```

An explicit `local-test/.env` file is recommended to make bind mounts
reproducible across terminal sessions.

Inspect the resolved Compose configuration before creating containers,
particularly when using `podman-compose`.

### Starting the environment

From the orchestrator module directory:

```bash
podman compose \
  --env-file local-test/.env \
  -f local-test/compose.yaml \
  --profile analyser up -d
```

The analyser is selected through the `analyser` profile.

If an existing service only needs to be restarted, use `podman restart`
against its container rather than recreating the entire Compose project.

### Submitting a validation

Use an existing datapackage with a UUID matching its repository directory.

```bash
python3 local-test/trigger.py --uuid <uuid>
```

The script seeds a mock validation record containing the persisted workflow
steps and publishes the initial RabbitMQ message.

The mock normalizes UUIDs, stores validation records in memory, and logs
state updates and report uploads.

### Viewing application logs

```bash
podman compose \
  -f local-test/compose.yaml \
  --profile analyser \
  logs -f --tail=100 \
  analyser orchestrator mock-validator-ws
```

RabbitMQ logs are excluded from this command to make application events
easier to inspect.

### Verified integration results

The following scenarios have been exercised against the real analyser:

| Scenario | Analyser result | Workflow result | Report |
|----------|-----------------|-----------------|--------|
| Valid datapackage | `valid=true` | `FINISHED` | Uploaded |
| Invalid datapackage | `valid=false` | `FAILED` | Uploaded |

The mock WS confirmed successful HTTP report uploads using report type
`DWCDP_ANALYSIS`.

This verifies the initial request, analyser dispatch, completion handling,
report-upload hook, and persisted terminal status.

It does not establish production-safe concurrent processing, exactly-once
dispatch, or full multi-step orchestration.

## 10. Build and Docker image

The orchestrator is a Java 17 Spring Boot application.

Build from the parent repository:

```bash
mvn -pl gbif/validator/validator-orchestrator \
    -am clean package -DskipTests
```

Run tests:

```bash
mvn -pl gbif/validator/validator-orchestrator \
    -am verify
```

The executable JAR is packaged for use through `java -jar`.

The Docker build uses a Java 17 runtime image and the packaged
orchestrator artifact.

From the repository root:

```bash
build/validator-orchestrator-docker-build.sh false <maven-version>
```

The Docker build script expects an executable artifact matching the
`*-shaded.jar` filename pattern in the orchestrator module's `target/`
directory.

The build configuration must produce that artifact before running the
Docker build script.

The Jenkins pipeline follows the repository's existing image-build pattern
and publishes versioned images to:

```text
docker.gbif.org/validator-orchestrator:<version>
```

The image build does not itself deploy or enable the RabbitMQ consumers.

## 11. Configuration

Runtime configuration is defined primarily in:

```text
src/main/resources/application.yml
```

Important configuration categories include:

- RabbitMQ host, port, username, password, and virtual host.
- Initial-request exchange, queue, routing key, and consumer settings.
- Step-specific request destinations.
- Completion exchange, queue, routing key, and consumer settings.
- `validator-ws` connectivity.
- Workflow nodes, dependencies, and join behavior.
- Development-only consumer-enable flags.

RabbitMQ connection configuration uses `postal-api` directly rather than
Spring AMQP's connection infrastructure.

Environment-specific values should be supplied through deployment
configuration.

### Logging

Spring Boot provides console logging through its default logging system.

The current integration has exposed the need for more explicit operational
logging in the orchestration classes.

Planned improvements include:

- Initial request accepted.
- Workflow loaded.
- Step dispatched.
- Completion received and interpreted.
- Report upload succeeded or failed.
- Step state transition.
- Workflow terminal state.
- Failed infrastructure interactions.

The intended production convention is to log workflow lifecycle events at
`INFO` and internal workflow evaluation details at `DEBUG`.

Deployment-specific Logback configuration can be supplied using
`LOGGING_CONFIG` or the corresponding Spring Boot system property.

## 12. Tests and verification

Unit tests cover the orchestration model and integration adapters using
JUnit 5.

Tests use handwritten fakes and recording collaborators instead of
Mockito or AssertJ.

Relevant coverage includes:

- Workflow dependency evaluation.
- Step dispatch selection.
- Completion processing.
- Duplicate completion handling.
- Validation report hook behavior.
- Failed report upload handling.
- Invalid datapackage results.
- Persisted workflow updates.

`DwcdpExecutionFlowTest` covers the integration flow using in-memory
collaborators.

It does not start RabbitMQ or make real HTTP requests.

The `local-test/` environment complements unit tests with actual RabbitMQ,
HTTP communication, and the published analyser image.

## 13. Current limitations and next steps

### Implemented and verified

- Generic orchestration service and DAG workflow engine.
- Configurable step registration and dispatch.
- Initial validation request handling.
- Typed completion listener registration.
- DwC-DP analyser request and completion handling.
- Required validation report upload hook.
- Successful and invalid-data workflow outcomes.
- Local integration environment with the real analyser.

### Remaining work

1. **Observability**
   Add consistent workflow lifecycle logging and configurable Logback defaults.

2. **Failure handling**
   Test analyser execution errors, report upload failures, unavailable
   `validator-ws`, and malformed or rejected messages.

3. **Infrastructure recovery**
   Verify startup behavior, RabbitMQ reconnection, and consumer recovery.

4. **Concurrency and atomicity**
   Define a safe strategy for concurrent workflow updates and message dispatch,
   including handling crashes between HTTP writes and RabbitMQ publication.

5. **Validation pipeline integration**
   Wire the remaining validation steps or an explicit handoff to the existing
   validation pipeline.

6. **Production validator-ws compatibility**
   Verify the report endpoints and status persistence against the actual
   deployed WS implementation.

7. **DAG skip persistence**
   Align the shared validation status model with `SKIPPED` before enabling
   persistent skip transitions.

8. **Deployment verification**
   Validate CI packaging, environment-specific RabbitMQ routing, HTTP
   connectivity, and consumer-enable configuration.

## 14. Operational guarantees

The current application provides a working integration foundation, not a
fully transactional workflow execution system.

Specifically:

- Workflow execution is not exactly-once.
- RabbitMQ dispatch and HTTP state persistence are not atomic.
- Automatic workflow retries are intentionally disabled.
- Message rejection does not guarantee persisted failure state.
- Concurrent updates are not yet protected by a demonstrated locking or
  optimistic-concurrency strategy.
- Full validation-pipeline handoff is not implemented.

These limitations must be addressed or explicitly accepted before enabling
unrestricted production consumers.
