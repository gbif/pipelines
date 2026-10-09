# Changelog

## [4.0.43] - Unreleased

### Added

- Standalone Spring Boot orchestrator bootstrap and external RabbitMQ connection configuration.
- Per-`StepType` request and completion routing configuration, including explicit consumer queue bindings.
- Existing `ValidationWsClient` integration for reading/updating persisted validation workflow state.
- Pure sequential workflow engine with events, decisions, status mapping and JUnit 5 behavior tests.
- Configurable step dispatcher and initial DWc-DP analyser request factory.
- DWc-DP completion interpreter, handler and `postal-api` callback with unchecked exception propagation.
- Optional DWc-DP completion listener, disabled by default pending concurrency and recovery safeguards.

### Changed

- Consolidated earlier version-specific documentation into `README.md`.
- Normalized Java imports, whitespace and control-flow braces in the affected implementation classes.

### Known limitations

- No production-safe atomic step claiming or reliable dispatch recovery.
- No persistence for the full analyser validation report yet.
- No complete end-to-end run or Maven compilation verified against the repository.
- Existing validation start-message migration and further step adapters are pending.
