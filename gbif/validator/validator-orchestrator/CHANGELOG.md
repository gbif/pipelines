# Changelog

## 4.0.43 — Initial version

### Added

- Spring Boot orchestrator with configurable RabbitMQ connections, exchanges, routing keys, and queues.
- Integration with the existing validation web-service client for workflow state.
- Pure sequential and DAG workflow evaluation, dependency validation, configurable join policies, and opt-in parallel scheduling.
- Typed step definitions, step registry, completion adapters, required and optional hooks, and workflow-change proposals.
- DWc-DP analyser request and completion handling, including a required validation-report upload hook using string report-type identifiers.
- Initial validation request listener and validation state-transition coordination.
- Generic typed completion listener registration, currently enabled only for DWc-DP analysis.
- JUnit 5 tests for workflow decisions, step registration, report handling, and multi-component DWc-DP execution, using handwritten test doubles.
- Reusable `RecordingValidationClient` test fake.
- Terminal workflow failure when a required completion hook cannot finish; no automatic retry.
- Java 17 container packaging and Jenkins Docker image publication.

### Known limitations

- Automatic consumers are disabled by default pending reliable dispatch and state-persistence coordination.
- Live end-to-end execution against RabbitMQ, the analyser, and validator-ws remains unverified.
- `SKIPPED` persistence and deployment-specific routing require integration validation.
- Legacy pipeline handoff after DWc-DP analysis is not wired; later steps remain pending.
- No automatic workflow retry; reruns are separate, manually invoked executions.
