# Spark Coordinator

The spark-coordinator module is responsible for executing GBIF pipeline steps in response to
RabbitMQ messages. Each running instance handles one pipeline step in one of two execution modes:

- **Standalone** — runs the Spark pipeline in-process (embedded Spark, suitable for smaller datasets)
- **Distributed** — submits the pipeline to Apache Airflow which schedules it as a Spark job on Kubernetes

A single instance handles exactly one `--mode`, consuming from one queue. Multiple instances run
in parallel across different modes and queues to form the full pipeline.

## Architecture

```
RabbitMQ queue
      │
      ▼
 Coordinator (mode=X)
      │
      ├─── Standalone: runs Spark pipeline in-process
      │         └── publishes outgoing message to balancer on success
      │
      └─── Distributed: submits DAG to Airflow, polls until complete
                └── publishes outgoing message to balancer on success
```

After each successful step, the coordinator publishes an outgoing message back to the
`PipelinesBalancerMessage` queue, which triggers the next step in the pipeline.
Pipeline step status (`RUNNING`, `COMPLETED`, `FAILED`) is reported to the GBIF registry
throughout for monitoring.

## Running

```bash
java --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  -jar target/spark-coordinator.jar \
  --mode=INTERPRETATION \
  --queueName=pipelines_occurrence_interpretation_standalone \
  --routingKey=occurrence.pipelines.verbatim.finished.standalone \
  --config=/path/to/pipelines-spark.yaml \
  --listenerThreads=1
```

## Available modes

Each mode has a standalone and distributed variant. Standalone initialises an embedded Spark
session on startup; distributed only needs a connection to Airflow and HDFS.

| Mode                                | Pipeline | Standalone Spark |
|-------------------------------------|---|---|
| `IDENTIFIER`                        | `IdentifiersPipeline` | Yes |
| `IDENTIFIER_DISTRIBUTED`            | `IdentifiersPipeline` via Airflow | No |
| `INTERPRETATION`                    | `OccurrenceInterpretationPipeline` | Yes |
| `INTERPRETATION_DISTRIBUTED`        | `OccurrenceInterpretationPipeline` via Airflow | No |
| `EVENTS_INTERPRETATION`             | `EventInterpretationPipeline` | Yes |
| `EVENTS_INTERPRETATION_DISTRIBUTED` | `EventInterpretationPipeline` via Airflow | No |
| `TABLEBUILD`                        | `TableBuildPipeline` (occurrence) | Yes |
| `TABLEBUILD_DISTRIBUTED`            | `TableBuildPipeline` (occurrence) via Airflow | No |
| `EVENTS_TABLEBUILD`                 | `TableBuildPipeline` (event) | Yes |
| `EVENTS_TABLEBUILD_DISTRIBUTED`     | `TableBuildPipeline` (event) via Airflow | No |
| `INDEXING`                          | `IndexingPipeline` (occurrence) | Yes |
| `INDEXING_DISTRIBUTED`              | `IndexingPipeline` (occurrence) via Airflow | No |
| `EVENTS_INDEXING`                   | `IndexingPipeline` (event) | Yes |
| `EVENTS_INDEXING_DISTRIBUTED`       | `IndexingPipeline` (event) via Airflow | No |
| `FRAGMENTER`                        | `FragmenterPipeline` | Yes |
| `FRAGMENTER_DISTRIBUTED`            | `FragmenterPipeline` via Airflow | No |
| `OCCURRENCE_DELETION`               | Deletes occurrence dataset from Iceberg + ES + HDFS | Yes |
| `EVENT_DELETION`                    | Deletes event dataset from Iceberg + ES + HDFS | Yes |
| `DWCDP_STAGE_STANDALONE`            | `DataPackageConversionPipeline` | Yes |
| `DWCDP_STAGE_DISTRIBUTED`           | `DataPackageConversionPipeline` via Airflow | No |
| `DWCDP_TO_VERBATIM_STANDALONE`      | `DwcDpToVerbatimPipeline` | Yes |
| `DWCDP_TO_VERBATIM_DISTRIBUTED`     | `DwcDpToVerbatimPipeline` via Airflow | No |

## DwC-DP pipeline flow

DwC-DP datasets follow a two-step ingestion path before joining the standard interpretation pipeline:

```
DwcDpMetadataSyncFinishedMessage
      │
      ▼ balancer (DwcDpStageMessageHandler)
      │   reads datapackage.json from NFS, determines workflow graph from
      │   containsOccurrences/containsEvents, resolves full step set via
      │   PipelinesWorkflow.getWorkflow().getAllNodesFor(DWCDP_STAGE)
      │
      ▼ DWCDP_STAGE_{STANDALONE|DISTRIBUTED}
      │   DataPackageConversionPipeline: copies DwC-DP CSV/TSV/Parquet
      │   from NFS to HDFS as partitioned Parquet
      │
      ▼ balancer (DwcDpToVerbatimMessageHandler)
      │   reads NFS archive size to decide standalone vs distributed
      │
      ▼ DWCDP_TO_VERBATIM_{STANDALONE|DISTRIBUTED}
      │   DwcDpToVerbatimPipeline: joins event/occurrence/media tables
      │   from HDFS Parquet, writes verbatim.avro
      │
      ▼ balancer (VerbatimMessageHandler or EventsMessageHandler)
      │   depending on containsOccurrences/containsEvents:
      │     occurrence (± events) → PipelinesVerbatimMessage → VERBATIM_TO_IDENTIFIER
      │     events only           → PipelinesEventsMessage   → EVENTS_VERBATIM_TO_INTERPRETED
      │
      ▼ standard GBIF interpretation pipeline continues...
```

## CLI arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `--mode` | Yes | — | One of the modes listed above |
| `--config` | No | `/tmp/pipelines-spark.yaml` | Path to `pipelines-spark.yaml` |
| `--queueName` | Yes | — | RabbitMQ queue to consume from |
| `--routingKey` | Yes | — | RabbitMQ routing key to bind to |
| `--exchange` | No | `occurrence` | RabbitMQ exchange |
| `--listenerThreads` | No | `1` | Number of parallel messages to process |
| `--master` | No | `local[*]` | Spark master, relevant for standalone modes only |
| `--listenerThreadSleepMillis` | No | `1000` | Sleep interval for the listener loop |
| `--prometheusPort` | No | `9404` | Prometheus metrics port, set to `0` to disable |

## Callback pattern

All pipeline steps follow the same pattern via `PipelinesCallback`:

1. Message received from queue
2. Pipeline step marked as `RUNNING` in the registry
3. `runPipeline()` executes the pipeline (in-process or via Airflow)
4. Pipeline step marked as `COMPLETED` or `FAILED` in the registry
5. Outgoing message published to the balancer queue to trigger the next step
6. Execution marked as finished in the registry if all steps are done

The distributed variants extend their standalone counterparts and override only `runPipeline()`,
delegating to `DistributedUtil.runPipeline()` which reads the record count from HDFS metrics,
selects a `SparkJobConfig` based on dataset size, and submits an Airflow DAG run via
`AirflowSparkLauncher`.

### Lifecycle controls

Two sentinel files control the coordinator process without requiring a restart:

| File | Effect |
|---|---|
| `/tmp/pause_message_processing` | Pauses consumption of new messages, existing processing continues |
| `/tmp/shutdown_now` | Stops accepting new messages, process exits after current work completes |

## Configuration

The coordinator reads `pipelines-spark.yaml` (via `--config`). The key sections relevant to the
coordinator are:

```yaml
standalone:
  messaging:
    host: rabbitmq.gbif.org
    virtualHost: /prod
    username: guest
    password: guest
    port: 5672
    prefetchCount: 1
  registry:
    wsUrl: http://api.gbif.org/v1
    user: username
    password: secret
  numberOfShards: 10
  occurrenceIndexAlias: occurrence
  occurrenceIndexSchema: /path/to/occurrence-schema.json
  occurrenceIndexNumberOfShards: 3
  eventIndexAlias: event
  eventIndexSchema: /path/to/event-schema.json
  eventIndexNumberOfShards: 3

airflowConfig:
  address: http://airflow.gbif.org
  user: airflow
  pass: secret
  apiCheckDelaySec: 10
  interpretationDag: gbif-occurrence-interpretation
  identifierDag: gbif-occurrence-identifier
  eventsInterpretationDag: gbif-events-interpretation
  tableBuildDag: gbif-occurrence-tablebuild
  eventsTableBuildDag: gbif-events-tablebuild
  indexingDag: gbif-occurrence-indexing
  eventsIndexingDag: gbif-events-indexing
  fragmenterDag: gbif-fragmenter
  dwcDpNfsToHdfsDag: gbif-pipelines-dwc-dp-nfs-to-hdfs
  dwcDpToVerbatimDag: gbif-pipelines-dwc-dp-to-verbatim

# Spark resource configs keyed by record count expression.
# Used by distributed modes to select executor sizing.
processingConfigs:
  "0 <= x < 1_000_000":
    executorInstances: 2
    executorCores: 4
    driverCores: 2
    # ... etc
  "1_000_000 <= x":
    executorInstances: 8
    executorCores: 8
    driverCores: 4
    # ... etc

hdfsSiteConfig: /etc/hadoop/conf/hdfs-site.xml
coreSiteConfig: /etc/hadoop/conf/core-site.xml
outputPath: hdfs://namenode/data/pipelines-output
inputPath: hdfs://namenode/data/pipelines-data

# DwC-DP specific
dwcdpNfsRepository: /mnt/nfs/dwcdp
partitionSizeInMB: 256
```

## Prometheus metrics

Each instance exposes metrics on `--prometheusPort` (default `9404`):

| Metric | Type | Description |
|---|---|---|
| `last_consumed_message_timestamp_milliseconds` | Gauge | Timestamp of last message taken from queue |
| `last_completed_dataset_timestamp_milliseconds` | Gauge | Timestamp of last successfully completed dataset |
| `concurrent_datasets` | Gauge | Number of datasets currently being processed |
| `messages_read_from_queue` | Counter | Total messages consumed |
| `completed_datasets` | Counter | Total datasets completed successfully |
| `datasets_errored_count` | Counter | Total errors during processing |
| `last_dataset_error_timestamp_milliseconds` | Gauge | Timestamp of last error |
