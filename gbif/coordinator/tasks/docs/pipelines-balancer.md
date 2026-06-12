# Pipelines Balancer

The balancer listens to the `PipelinesBalancerMessage` queue and routes each dataset to either a standalone (in-process Spark) or distributed (Airflow-submitted Spark) execution path, based on configurable thresholds. It does not run any pipeline logic itself.

## How it works

1. A message arrives on the configured queue
2. The balancer inspects the message type and applies the relevant handler
3. Each handler checks a size threshold (file size in bytes, or record count) against the dataset
4. The message is re-published to either a `*.standalone` or `*.distributed` routing key on the `occurrence` exchange
5. Downstream coordinator services consume from those routing keys

Pipeline step status (`RUNNING`, `COMPLETED`, `FAILED` etc.) is reported back to the GBIF registry throughout, so progress is visible on each dataset's pipeline tracking record.

## Running

```bash
java --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  -jar target/pipelines-coordinator-cli.jar pipelines-balancer \
  --log-config /path/to/logback-pipelines-balancer.xml \
  --conf /path/to/pipelines-balancer.yaml
```

## Configuration

Configuration is supplied as a YAML file via `--conf`. All fields map directly to CLI parameters but YAML is the standard way to supply them in production.

### Full example

```yaml
stepConfig:
  registry:
    wsUrl: http://api.gbif.org/v1     # default
    user: username
    password: secret
  messaging:
    host: rabbitmq.gbif.org
    virtualHost: /vhost
    username: guest
    password: guest
    port: 5672                          # optional, default 5672
  queueName: pipelines-balancer
  poolSize: 1
  hdfsSiteConfig: /etc/hadoop/conf/hdfs-site.xml
  coreSiteConfig: /etc/hadoop/conf/core-site.xml
  repositoryPath: hdfs://namenode/data/ingest
  eventsEnabled: false                  # optional, default false

switchFileSizeMb: 512                   # datasets above this go to distributed
validatorSwitchRecordsNumber: 100000    # validator threshold (record count)
validatorRepositoryPath: /nfs/validator # NFS path for validator archives
dwcDpRepositoryPath: /nfs/dwcdp/unpacked    # NFS path for DwC-DP unpacked archives
```

### Field reference

#### `stepConfig` (shared across all services)

The registry is used to report pipeline step status back to the GBIF registry API (`PipelineProcess` / `PipelineStep` records). Credentials are only needed if the registry requires authentication for status updates.

| Field | CLI flag | Required | Default | Description |
|---|---|---|---|---|
| `stepConfig.registry.wsUrl` | `--registry-ws` | No | `http://api.gbif.org/v1` | GBIF registry API base URL, used for pipeline step status reporting |
| `stepConfig.registry.user` | `--registry-user` | No | — | Registry API username |
| `stepConfig.registry.password` | `--registry-password` | No | — | Registry API password |
| `stepConfig.messaging.host` | `--messaging-host` | Yes | — | RabbitMQ hostname |
| `stepConfig.messaging.virtualHost` | `--messaging-virtual-host` | Yes | — | RabbitMQ virtual host |
| `stepConfig.messaging.username` | `--messaging-username` | Yes | — | RabbitMQ username |
| `stepConfig.messaging.password` | `--messaging-password` | Yes | — | RabbitMQ password |
| `stepConfig.messaging.port` | `--messaging-port` | No | `5672` | RabbitMQ port |
| `stepConfig.queueName` | `--queue-name` | Yes | — | Queue to consume from |
| `stepConfig.poolSize` | `--pool-size` | Yes | — | Number of concurrent messages to process |
| `stepConfig.hdfsSiteConfig` | `--hdfs-site-config` | Yes | — | Path to `hdfs-site.xml` |
| `stepConfig.coreSiteConfig` | `--core-site-config` | Yes | — | Path to `core-site.xml` |
| `stepConfig.repositoryPath` | `--repository-path` | Yes | — | HDFS base path for pipeline output |
| `stepConfig.eventsEnabled` | `--events-enabled` | No | `false` | Whether to route sampling-event datasets |

#### Balancer-specific fields

| Field                          | CLI flag                            | Required | Description |
|--------------------------------|-------------------------------------|---|---|
| `switchFileSizeMb`             | `--switch-file-size-mb`             | Yes | File size threshold in MB. Datasets whose unpacked NFS archive exceeds this are routed to distributed execution |
| `validatorSwitchRecordsNumber` | `--validator-switch-records-number` | Yes | Record count threshold for validator datasets |
| `validatorRepositoryPath`      | `--validator-repository-path`       | No | NFS path where the validator looks for archives to validate (e.g. DwC-A) |
| `dwcDpRepositoryPath`          | `--dwcdp-repository-path`           | No | NFS path containing unpacked DwC-DP archives, used for file size inspection |

## Message handlers

The balancer handles several message types. Each maps to a specific routing key pattern on the `occurrence` exchange.

### DwC-A / XML / ABCD (`PipelinesVerbatimMessage`)

Routes based on record count read from a metrics YAML on HDFS (`archive-to-verbatim.yml`).

| Routing key (outbound) | Condition |
|---|---|
| `pipelines.verbatim-to-interpreted.standalone` | record count ≤ threshold |
| `pipelines.verbatim-to-interpreted.distributed` | record count > threshold |

### DwC-DP NFS to HDFS (`DwcDpMetadataSyncFinishedMessage`)

Routes based on total file size of the unpacked archive on NFS (`dwcDpRepositoryPath/<datasetKey>`),
walked recursively. The full pipeline step set for the execution is resolved from the workflow
graph at this point via `PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
.getAllNodesFor(NFS_TO_HDFS)`, so downstream steps are registered in the registry from the start.

| Routing key (outbound) | Condition |
|---|---|
| `occurrence.dwcdp.nfs-to-hdfs.standalone` | total file size ≤ `switchFileSizeMb` |
| `occurrence.dwcdp.nfs-to-hdfs.distributed` | total file size > `switchFileSizeMb` |

### DwC-DP verbatim conversion (`DwcDpToVerbatimMessage`)

Routes based on total file size of the same NFS archive (`dwcDpRepositoryPath/<datasetKey>`).
Emits a `DwcDpToVerbatimMessage` with the `standalone` flag set, whose `getRoutingKey()` appends
the correct suffix automatically.

| Routing key (outbound) | Condition |
|---|---|
| `occurrence.dwcdp.to-verbatim.standalone` | total file size ≤ `switchFileSizeMb` |
| `occurrence.dwcdp.to-verbatim.distributed` | total file size > `switchFileSizeMb` |

### Validator (`PipelinesBalancerMessage` with validator step)

Routes based on record count against `validatorSwitchRecordsNumber`, using `validatorRepositoryPath` for archive inspection.
