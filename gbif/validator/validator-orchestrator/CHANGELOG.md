# Changelog

## 4.0.43 — Initial version

### Added

- Spring Boot orchestrator with configurable RabbitMQ connections, exchanges, routing keys, and queues.
- Integration with the existing validation web-service client for workflow state.
- Pure sequential and DAG workflow evaluation, dependency validation, configurable join policies, and opt-in parallel scheduling.
- Typed step definitions, step registry, completion adapters, required and optional hooks, and workflow-change proposals.
- DWc-DP analyser request and completion handling, including a required validation-report upload hook using string report-type identifiers.
- Initial validation request listener and validation state-transition coordination.
- Generic typed completion listeners registered from step definitions and configurable per-step queue bindings.
- Configurable validation worker adapters for DWc-DP stage, DWc-DP to-verbatim, and verbatim-to-identifier using proposed JSON contracts.
- JUnit 5 tests for workflow decisions, step registration, report handling, and multi-component DWc-DP execution, using handwritten test doubles.
- Reusable `RecordingValidationClient` test fake.
- Terminal workflow failure when a required completion hook cannot finish; no automatic retry.

### Known limitations

- Automatic consumers are disabled by default pending reliable dispatch and state-persistence coordination.
- Live end-to-end execution against RabbitMQ, the analyser, and validator-ws remains unverified.
- `SKIPPED` persistence and deployment-specific routing require integration validation.
- No automatic workflow retry; reruns are separate, manually invoked executions.
