# Validator Orchestrator — Local integration harness

Run commands from `gbif/validator/validator-orchestrator/local-test/`.
The checked-in `Taskfile.dist.yml` provides the commands below; `Taskfile.yml` is optional and reserved for uncommitted local tasks.

## Quick run

1. Build the orchestrator's executable `target/*-shaded.jar` using Maven from the parent repository.
2. Create `local-test/.env` (use **absolute** paths):

   ```dotenv
   DATAPACKAGES_DIR=/absolute/path/to/datapackages
   DWCDP_WORKDIR=/absolute/path/to/workdir
   ```

3. Run from `local-test/`:

   ```sh
   task build
   task up:analyser
   task trigger UUID=673f2308-3501-474f-85d5-a832825bb46c
   task logs
   ```

   Use the UUID of a datapackage **actually present** under `DATAPACKAGES_DIR`.
   `task logs` follows the orchestrator, analyser and mock WS, excluding RabbitMQ.

4. Check the validation record:

   ```sh
   curl http://127.0.0.1:18080/validation/673f2308-3501-474f-85d5-a832825bb46c
   ```

5. Stop the environment:

   ```sh
   task down
   ```

## Task reference

| Task | Purpose |
| --- | --- |
| `task build` | Build `localhost/validator-orchestrator:test` from the existing shaded JAR |
| `task up` | Start broker, mock WS and orchestrator without analyser |
| `task up:analyser` | Validate mount paths and start the full stack |
| `task trigger UUID=<uuid>` | Seed mock WS and submit an orchestration request |
| `task complete UUID=<uuid>` | Send a synthetic completion; **do not** use alongside a real analyser job for the same UUID |
| `task logs` | Follow application logs (not RabbitMQ) |
| `task logs:orchestrator` | Follow orchestrator logs |
| `task logs:analyser` | Follow analyser logs |
| `task status` | List local-test containers, including stopped ones |
| `task pull:analyser` | Pull the configured analyser image; does not replace a running container |
| `task restart:analyser` | Restart only the existing analyser container; retains image and mounts |
| `task down` | Stop and remove the Compose environment |

## Prerequisites and configuration

- Podman with a working Compose provider (`podman-compose` is supported).
- [Task](https://taskfile.dev/) (`go-task`).
- Python 3 for `trigger.py`.
- An executable shaded JAR built in `validator-orchestrator/target/`.
- A real DwC-DP datapackage repository for analyser-backed tests.

The Compose services are defined in `compose.yaml`. The orchestrator loads
`application-local.yml` from a read-only mount with Spring's `local` profile.
That file contains orchestration listener, step routing and development-only
consumer settings. The packaged `application.yml` continues to supply common
Spring/RabbitMQ defaults; Compose supplies broker and mock WS connection values.

Keep `.env` local to your machine (gitignored). Its paths select **host bind mounts**,
not paths inside the analyser. `task up:analyser` checks that the datapackage
repository exists and creates the writable workdir if needed, preventing
accidental empty-source mounts.

The analyser uses these paths:

| Host setting | Container path | Access |
| --- | --- | --- |
| `DATAPACKAGES_DIR` | `/data/datapackages` | Read-only |
| `DWCDP_WORKDIR` | `/data/workdir` | Read/write |

The analyser image defaults to
`docker.gbif.org/dwc-dp-analyser-service:0.0.7-SNAPSHOT` and can be changed
with `DWCDP_ANALYSER_IMAGE` in `.env`. The default local orchestrator image
is `localhost/validator-orchestrator:test`.

## Processing flow

```text
trigger.py
  |-- seed Validation + steps --> mock-validator-ws
  `-- publish orchestration request --> RabbitMQ
                                        |
                                        v
                                   orchestrator
                                        |-- GET persisted Validation --> mock-validator-ws
                                        `-- analyser request --> RabbitMQ
                                                                 |
                                                                 v
                                                         DwC-DP analyser
                                                         (mounted datapackage)
                                                                 |
                                               DwcDpValidationFinished
                                                                 |
                                                                 v
                                                           orchestrator
                                                             |-- PUT report --> mock-validator-ws
                                                             `-- PUT status --> mock-validator-ws
```

The analyser listens on queue `validator_dwcdp_analysis`, bound by `trigger.py`
to the `occurrence` topic exchange using `validator.dwcdp.analysis`.
It publishes completion events to exchange `occurrence` with routing key
`validator.dwcdp.analysis.finished`. The orchestrator completion listener
uses queue `validator_dwcdp_analysis_finished`. The initial orchestration
listener's queue/routing are defined in `application-local.yml`; keep them
aligned with `trigger.py` when changing them.

The analyser's Registry URL is empty, so it does not upload to Registry.
The orchestrator uploads the returned report to mock validator-ws using
report type `DWCDP_ANALYSIS`.

| Analyser outcome | Workflow result | Report |
| --- | --- | --- |
| `valid=true` | `FINISHED` | Uploaded |
| `valid=false` | `FAILED` | Uploaded |

## Datapackage input

The analyser resolves files relative to `/data/datapackages/<uuid>/`.
It supports unpacked datapackages at `<uuid>/<attempt>/datapackage.json`,
`<uuid>/<uuid>.<attempt>/datapackage.json`, or `<uuid>/datapackage.json`,
and attempt-specific `.dwcdp`/`.zip` archives. The datapackage must include
all files referenced by its descriptor.

Use lowercase UUID directory names, matching Java `UUID.toString()`.
The `task trigger` command does **not** create or copy input data.

## Inspect and troubleshoot

RabbitMQ management: <http://127.0.0.1:15672> (`localtest` / `localtest`).
Mock validator-ws: <http://127.0.0.1:18080>.

To enter the analyser container interactively:

```sh
podman exec -it "$(podman ps -q --filter name=validator-orchestrator-local_analyser)" /bin/sh
```

Once inside, inspect `/data/datapackages` and check the UUID/attempt path.
To inspect a bind mount from the host without starting other services:

```sh
podman inspect "$(podman ps -aq --filter name=validator-orchestrator-local_analyser)" \
  --format '{{range .Mounts}}{{println .Source "->" .Destination}}{{end}}'
```

`task restart:analyser` changes neither the image nor volume mounts. If those
change, the container must be recreated; **do not** substitute a broad Compose
restart that disrupts RabbitMQ and the orchestrator. Verify supported Compose
options before recreating only that service.

For mock-only completion testing:

```sh
task up
sleep 15
task up
task trigger UUID=<uuid>
task complete UUID=<uuid>
```

Do not issue synthetic and real completion messages for the same execution.

## Limitations

This is a controlled integration harness, not a production deployment.
It enables development-only orchestrator consumers, runs a single orchestrator,
and has no atomic transaction across validator-ws state updates and RabbitMQ
publishing. The mock implements only the WS endpoints needed for this flow.
The broader validation pipeline requires additional services not included here.
