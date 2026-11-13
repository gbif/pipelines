#!/usr/bin/env python3
"""End-to-end test of the HBase records tables on lab, with one dataset registered for the test.

Needs kubectl (context with access to the lab and test namespaces) and Python 3 (stdlib only;
with pyarrow installed, the keys in the manifests are counted too).

  1. Build the archives of N records and upload them where the lab crawler can download them,
     <archives-url>/v1.zip, v2.zip, v3.zip:

       python3 gbif/ingestion/spark-jobs/scripts/records_test.py build --records 100       # Puts
       python3 gbif/ingestion/spark-jobs/scripts/records_test.py build --records 200000    # HFiles bulk load (> bulkLoadIfRecordsMoreThan)

  2. Register the dataset and run the loads, checking the index, HBase and the manifests after each:

       export GBIF_USER=... GBIF_PASSWORD=...
       python3 gbif/ingestion/spark-jobs/scripts/records_test.py run --records 200000 --org <publishingOrganizationKey> \
           --installation <installationKey> --archives-url https://example.org/records-test

     With R = N / 5:
     v1  test-1..N                 first load: N rows, a current manifest only
     v2  test-(R+1)..(N+R)         test-1..R deleted from HBase, test-(N+1)..(N+R) added
     v3  same ids, count=5         same keys, rows rewritten by the new attempt, nothing deleted

     Every record is checked through the index count and the manifest key count (pyarrow), and a
     sample of records (all of them for small datasets) row by row in HBase: present with the
     attempt of the load, or deleted. The runner of each step (STANDALONE/DISTRIBUTED) and whether
     the load was bulk loaded are printed.

     The dataset key is kept in records_test.state (next to the script), a second `run` continues with the same
     dataset (--from-step 2 to start from v2). Use a new state file (--state) per dataset size.

     The crawler downloads with If-Modified-Since the last crawl: an archive whose Last-Modified
     is older is NOT_MODIFIED, even under a new URL. Touch each archive before its crawl with
     --before-crawl, or the script waits for you to touch it:

       --before-crawl "ssh labs.gbif.org touch public_html/test-archives/v{version}.zip"

     After each load the Elasticsearch documents are checked against indexConfig.sourceEnabled of
     the pipelines-spark-yaml config map: with true the documents keep verbatim in their _source,
     with false verbatim/multimediaItems aren't sent, and an index of the dataset alone (larger
     than bigIndexIfRecordsMoreThan) is created with the _source disabled. The default index keeps
     the mappings it was created with. Elasticsearch is read at the first esHosts of the config
     map, or --es-url.

  3. To test both settings, run the loads with indexConfig.sourceEnabled: true (the default), then
     set it to false in the pipelines-spark-yaml config map, restart the indexing services so they
     read it (DISTRIBUTED Spark jobs read the config map when they start) and crawl v3 again:

       python3 gbif/ingestion/spark-jobs/scripts/records_test.py run --records 200000 --from-step 3 ...

     Build the dataset larger than bigIndexIfRecordsMoreThan to have an index of its own, created
     with the new setting; in the default index only the fields sent are checked. `source` checks
     the documents of the dataset at any time, without crawling:

       python3 gbif/ingestion/spark-jobs/scripts/records_test.py source

  4. Delete the dataset in the registry and check its rows and manifests are removed:

       python3 gbif/ingestion/spark-jobs/scripts/records_test.py delete

  status prints the current state of the dataset at any time:

       python3 gbif/ingestion/spark-jobs/scripts/records_test.py status

HDFS is read through HttpFS (lab/svc/httpfs-nodeport, port-forwarded), HBase with the hbase shell
of test/gbif-hbase-master-default-0 (kubectl exec). Nothing is written to HDFS or HBase.
"""
import argparse
import atexit
import base64
import io
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime
from email.utils import parsedate_to_datetime

API = "https://api.gbif-lab.org/v1"
TABLE = "lab_occurrence"
OUTPUT_PATH = "/data/ingest_lab"
MANIFESTS = f"{OUTPUT_PATH}/records-manifests"
HFILE_DIR = "records-hfile"
HTTPFS_PORT = 14000
HBASE_POD = ["-n", "test", "gbif-hbase-master-default-0", "-c", "hbase"]
# the archives and the state are next to the script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVES_DIR = os.path.join(SCRIPT_DIR, "records-test-archives")
NAMESPACE = "lab"
# fields mapped with "enabled": false, only kept in the _source
UNINDEXED_FIELDS = {"verbatim", "multimediaItems"}
# documents checked in Elasticsearch per load
ES_SAMPLE_SIZE = 5

# records checked row by row in HBase, per group (loaded, added, removed)
SAMPLE_SIZE = 300
LOOKUP_BATCH = 100

POLL_SECONDS = 30

state_file = os.path.join(SCRIPT_DIR, "records_test.state")
es_url_override = None


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def fail(msg):
    log(f"FAIL: {msg}")
    sys.exit(1)


# ---------------------------------------------------------------------------- versions

def versions(records):
    """version -> (first id, last id, individualCount)"""
    removed = max(1, records // 5)
    return {
        1: (1, records, 1),
        2: (removed + 1, records + removed, 1),
        3: (removed + 1, records + removed, 5),
    }


def id_range(records, version):
    first, last, _ = versions(records)[version]
    return range(first, last + 1)


def sample(ids):
    """Evenly spread sample of the ids, with the first and the last"""
    ids = list(ids)
    if len(ids) <= SAMPLE_SIZE:
        return ids
    step = len(ids) / (SAMPLE_SIZE - 1)
    return sorted({ids[min(int(i * step), len(ids) - 1)] for i in range(SAMPLE_SIZE)} | {ids[-1]})


def occurrence_id(i):
    return f"test-{i}"


# ---------------------------------------------------------------------------- archives

META = """<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
  <core encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" fieldsEnclosedBy=""
        ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
    <files><location>occurrence.txt</location></files>
    <id index="0"/>
    <field index="0" term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
    <field index="1" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
    <field index="2" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
    <field index="3" term="http://rs.tdwg.org/dwc/terms/eventDate"/>
    <field index="4" term="http://rs.tdwg.org/dwc/terms/decimalLatitude"/>
    <field index="5" term="http://rs.tdwg.org/dwc/terms/decimalLongitude"/>
    <field index="6" term="http://rs.tdwg.org/dwc/terms/countryCode"/>
    <field index="7" term="http://rs.tdwg.org/dwc/terms/individualCount"/>
  </core>
</archive>
"""

EML = """<?xml version="1.0" encoding="UTF-8"?>
<eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1" packageId="records-test" system="http://gbif.org">
  <dataset><title>HBase records test</title>
    <creator><individualName><surName>Test</surName></individualName></creator>
    <contact><individualName><surName>Test</surName></individualName></contact>
  </dataset>
</eml:eml>
"""


def build_archives(records):
    os.makedirs(ARCHIVES_DIR, exist_ok=True)
    for version, (first, last, count) in versions(records).items():
        path = os.path.join(ARCHIVES_DIR, f"v{version}.zip")
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("meta.xml", META)
            z.writestr("eml.xml", EML)
            with z.open("occurrence.txt", "w", force_zip64=True) as f:
                f.write(
                    b"occurrenceID\tbasisOfRecord\tscientificName\teventDate\tdecimalLatitude"
                    b"\tdecimalLongitude\tcountryCode\tindividualCount\n"
                )
                for i in range(first, last + 1):
                    # coordinates within Denmark, varied so records aren't identical
                    lat = 55 + (i % 10000) / 10000
                    lon = 9 + (i % 30000) / 10000
                    f.write(
                        f"{occurrence_id(i)}\tHUMAN_OBSERVATION\tPuma concolor"
                        f"\t2020-01-{i % 28 + 1:02d}\t{lat:.4f}\t{lon:.4f}\tDK\t{count}\n".encode()
                    )
        size = os.path.getsize(path) / 1024 / 1024
        log(f"{path}: test-{first}..test-{last} ({last - first + 1} records, "
            f"individualCount={count}, {size:.1f} MB)")
    log(f"Upload {ARCHIVES_DIR}/v*.zip so that <archives-url>/v1.zip etc. can be downloaded by the crawler")


# ---------------------------------------------------------------------------- state

def load_state():
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------- registry / API

def http(method, url, body=None, auth=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if auth:
        user, password = os.environ.get("GBIF_USER"), os.environ.get("GBIF_PASSWORD")
        if not user or not password:
            fail("GBIF_USER and GBIF_PASSWORD must be set")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            text = resp.read().decode()
            return json.loads(text) if text.strip() else None
    except urllib.error.HTTPError as e:
        fail(f"{method} {url} -> {e.code} {e.read().decode()[:500]}")


def archive_last_modified(url):
    """Last-Modified of the archive, in seconds"""
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as r:
            if r.status != 200:
                fail(f"{url} -> {r.status}")
            header = r.headers.get("Last-Modified")
            return parsedate_to_datetime(header).timestamp() if header else None
    except urllib.error.URLError as e:
        fail(f"Archive not downloadable: {url} ({e})")


def crawls(dataset):
    """(started, finishReason) of the latest crawls of the dataset, started in seconds"""
    page = http("GET", f"{API}/dataset/{dataset}/process?limit=10")
    return [
        (parse_time(p["startedCrawling"]), p.get("finishReason"))
        for p in page.get("results", [])
        if p.get("startedCrawling")
    ]


def parse_time(value):
    if isinstance(value, (int, float)):
        return value / 1000
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def ensure_newer_than_last_crawl(state, url, version, before_crawl):
    """The crawler downloads with If-Modified-Since the last crawl: an archive older than that is
    NOT_MODIFIED, even under a new URL"""
    if before_crawl:
        cmd = before_crawl.format(version=version)
        log(f"Running: {cmd}")
        subprocess.run(cmd, shell=True, check=True)
    last_crawl = state.get("lastCrawl") or max((s for s, _ in crawls(state["dataset"])), default=None)
    while True:
        modified = archive_last_modified(url)
        if last_crawl is None or modified is None or modified > last_crawl:
            return
        log(f"{url} last modified {datetime.fromtimestamp(modified)}, before the last crawl "
            f"{datetime.fromtimestamp(last_crawl)}: the crawler would skip it as NOT_MODIFIED")
        input(f"Touch v{version}.zip on the server (or use --before-crawl) and press Enter... ")


def register_dataset(org, installation, records):
    title = f"HBase records test {records} records {datetime.now():%Y-%m-%d %H:%M}"
    key = http(
        "POST",
        f"{API}/dataset",
        {
            "type": "OCCURRENCE",
            "title": title,
            "publishingOrganizationKey": org,
            "installationKey": installation,
            "language": "eng",
            "license": "http://creativecommons.org/licenses/by/4.0/legalcode",
        },
        auth=True,
    )
    log(f"Registered dataset {key} ({title})")
    return key


def set_endpoint(state, url):
    dataset = state["dataset"]
    if state.get("endpoint"):
        http("DELETE", f"{API}/dataset/{dataset}/endpoint/{state['endpoint']}", auth=True)
    state["endpoint"] = http(
        "POST", f"{API}/dataset/{dataset}/endpoint", {"type": "DWC_ARCHIVE", "url": url}, auth=True
    )
    save_state(state)
    log(f"Endpoint {state['endpoint']}: {url}")


def crawl(dataset):
    http("POST", f"{API}/dataset/{dataset}/crawl", auth=True)
    log(f"Crawl requested for {dataset}")


def index_count(dataset):
    return http("GET", f"{API}/occurrence/search?datasetKey={dataset}&limit=0")["count"]


def lookup_gbif_ids(dataset, ids):
    """occurrenceID -> gbifId for the given ids the index returns"""
    result = {}
    for start in range(0, len(ids), LOOKUP_BATCH):
        batch = ids[start:start + LOOKUP_BATCH]
        params = "&".join(f"occurrenceId={urllib.parse.quote(occurrence_id(i))}" for i in batch)
        page = http("GET", f"{API}/occurrence/search?datasetKey={dataset}&{params}&limit={len(batch)}")
        for r in page["results"]:
            result[r.get("occurrenceID")] = r["key"]
    return result


def index_steps(dataset, started):
    """(type, runner, state) of the steps of the execution started after the given time"""
    page = http("GET", f"{API}/pipelines/history/{dataset}?limit=5")
    steps = []
    for process in page.get("results", []):
        for execution in process.get("executions", []):
            if execution.get("created") and parse_time(execution["created"]) >= started - 60:
                steps += [(s["type"], s.get("runner"), s["state"]) for s in execution.get("steps", [])]
    return steps


# ---------------------------------------------------------------------------- HDFS (HttpFS)

_port_forward = None


def httpfs():
    global _port_forward
    if _port_forward is None:
        _port_forward = subprocess.Popen(
            ["kubectl", "port-forward", "-n", "lab", "svc/httpfs-nodeport", f"{HTTPFS_PORT}:14000"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        atexit.register(_port_forward.terminate)
        time.sleep(3)
    return f"http://localhost:{HTTPFS_PORT}/webhdfs/v1"


def hdfs_status(path):
    """FileStatus of a path, None if it doesn't exist"""
    try:
        with urllib.request.urlopen(f"{httpfs()}{path}?op=GETFILESTATUS&user.name=hdfs", timeout=30) as r:
            return json.load(r)["FileStatus"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def hdfs_list(path):
    try:
        with urllib.request.urlopen(f"{httpfs()}{path}?op=LISTSTATUS&user.name=hdfs", timeout=30) as r:
            return [f["pathSuffix"] for f in json.load(r)["FileStatuses"]["FileStatus"]]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise


def hdfs_read(path):
    with urllib.request.urlopen(f"{httpfs()}{path}?op=OPEN&user.name=hdfs", timeout=300) as r:
        return r.read()


def manifests(dataset):
    """state -> modification time (ms) of the manifest of the dataset, for the existing ones"""
    result = {}
    for state in ["", "_pending", "_previous", "_stale"]:
        status = hdfs_status(f"{MANIFESTS}/occurrence{state}/datasetKey={dataset}")
        if status:
            result[state or "current"] = status["modificationTime"]
    return result


def manifest_keys(dataset):
    """Keys in the current manifest of the dataset, None without pyarrow"""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    path = f"{MANIFESTS}/occurrence/datasetKey={dataset}"
    return sum(
        pq.read_metadata(io.BytesIO(hdfs_read(f"{path}/{name}"))).num_rows
        for name in hdfs_list(path)
        if name.endswith(".parquet")
    )


def print_manifests(dataset):
    found = manifests(dataset)
    for state in ["current", "_pending", "_previous", "_stale"]:
        if state in found:
            extra = ""
            if state == "_stale":
                extra = " runs: " + ", ".join(hdfs_list(f"{MANIFESTS}/occurrence_stale/datasetKey={dataset}"))
            log(f"  manifest {state:10} {datetime.fromtimestamp(found[state] / 1000)}{extra}")
        else:
            log(f"  manifest {state:10} -")


# ---------------------------------------------------------------------------- HBase

def row_key(gbif_id):
    return f"{gbif_id % 100:02d}:{gbif_id}"


def hbase_shell(script):
    proc = subprocess.run(
        ["kubectl", "exec", "-i", *HBASE_POD, "--", "/stackable/hbase/bin/hbase", "shell", "-n"],
        input=script,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        fail(f"hbase shell failed:\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    return proc.stdout


def hbase_attempts(gbif_ids):
    """gbifId -> m:attempt of its row, None when the row doesn't exist"""
    script = "".join(
        f"puts 'KEY {g}'\nget '{TABLE}', '{row_key(g)}', 'm:attempt'\n" for g in gbif_ids
    )
    attempts, current = {g: None for g in gbif_ids}, None
    for line in hbase_shell(script).splitlines():
        m = re.match(r"^KEY (\d+)$", line.strip())
        if m:
            current = int(m.group(1))
            continue
        m = re.search(r"value=(\S+)", line)
        if m and current is not None:
            attempts[current] = m.group(1)
    return attempts


def hbase_interpreted(gbif_id):
    return hbase_shell(f"get '{TABLE}', '{row_key(gbif_id)}', 'd:interpreted'\n")


# ---------------------------------------------------------------------------- pipelines configuration

def spark_yaml():
    """pipelines-spark.yaml of the config map, read each time as it can be changed during the test"""
    proc = subprocess.run(
        ["kubectl", "get", "cm", "-n", NAMESPACE, "pipelines-spark-yaml",
         "-o", r"jsonpath={.data.pipelines-spark\.yaml}"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        fail(f"Can't read the pipelines-spark-yaml config map: {proc.stderr}")
    return proc.stdout


def config_section(name, yaml=None):
    """Lines of a top-level section of the pipelines configuration"""
    lines, inside = [], False
    for line in (yaml if yaml is not None else spark_yaml()).splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line[0].isspace():
            inside = line.split(":")[0].strip() == name
        elif inside:
            lines.append(line)
    return "\n".join(lines)


def config_value(section, key, yaml=None):
    m = re.search(rf"^\s*{key}:\s*(\S+)", config_section(section, yaml), re.M)
    return m.group(1).strip("'\"") if m else None


def config_int(section, key, default):
    value = config_value(section, key)
    return int(value.replace("_", "")) if value else default


def bulk_load_threshold():
    """bulkLoadIfRecordsMoreThan of the installed configuration"""
    return config_int("recordsTableConfig", "bulkLoadIfRecordsMoreThan", 50_000)


def source_enabled():
    """indexConfig.sourceEnabled of the installed configuration, true by default"""
    value = config_value("indexConfig", "sourceEnabled")
    return value is None or value.lower() == "true"


def occurrence_alias():
    return config_value("indexConfig", "occurrenceAlias") or "occurrence"


# ---------------------------------------------------------------------------- Elasticsearch

def es_url():
    if es_url_override:
        return es_url_override.rstrip("/")
    section = config_section("elastic")
    m = re.search(r"esHosts:(.*?)(?:\n\s*\w+:|\Z)", section, re.S)
    urls = re.findall(r"https?://[^\s,'\"\]]+", m.group(1)) if m else []
    if not urls:
        fail("No elastic.esHosts in the pipelines-spark-yaml config map, use --es-url")
    return urls[0].rstrip("/")


def es_indices(dataset):
    """index -> documents of the dataset, for the indices of the occurrence alias"""
    body = {
        "size": 0,
        "query": {"term": {"datasetKey": dataset}},
        "aggs": {"indices": {"terms": {"field": "_index", "size": 100}}},
    }
    result = http("POST", f"{es_url()}/{occurrence_alias()}/_search", body)
    return {b["key"]: b["doc_count"] for b in result["aggregations"]["indices"]["buckets"]}


def es_alias_count():
    return http("GET", f"{es_url()}/{occurrence_alias()}/_count")["count"]


def es_index_exists(index):
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{es_url()}/{index}", method="HEAD"), timeout=30):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def es_source_disabled(index):
    mappings = http("GET", f"{es_url()}/{index}/_mapping")[index]["mappings"]
    return mappings.get("_source", {}).get("enabled", True) is False


def check_index(dataset, gbif_ids, expected):
    """Checks the documents of the dataset against indexConfig.sourceEnabled, returns its index"""
    enabled = source_enabled()
    indices = es_indices(dataset)
    if sum(indices.values()) != expected:
        fail(f"{sum(indices.values())} documents of the dataset in {occurrence_alias()}, {expected} expected")
    if len(indices) != 1:
        fail(f"Documents of the dataset in several indices: {indices}")
    index = next(iter(indices))
    own = index.startswith(dataset)
    disabled = es_source_disabled(index)

    # an index of the dataset alone was created by this load, with the current configuration
    if own and disabled == enabled:
        fail(f"{index} has the _source {'disabled' if disabled else 'enabled'}, "
             f"indexConfig.sourceEnabled is {str(enabled).lower()}")
    if not own and disabled == enabled:
        log(f"  WARN default index {index} has the _source {'disabled' if disabled else 'enabled'}, "
            f"created before indexConfig.sourceEnabled was {str(enabled).lower()}")

    for gbif_id in sorted(gbif_ids)[:ES_SAMPLE_SIZE]:
        doc = http("GET", f"{es_url()}/{index}/_doc/{gbif_id}")
        if not doc.get("found"):
            fail(f"Document {gbif_id} not found in {index}")
        source = doc.get("_source")
        if disabled:
            if source is not None:
                fail(f"Document {gbif_id} has a _source in {index}, whose _source is disabled")
        elif enabled and "verbatim" not in (source or {}):
            fail(f"Document {gbif_id} sent without verbatim, indexConfig.sourceEnabled is true")
        elif not enabled and UNINDEXED_FIELDS & (source or {}).keys():
            fail(f"Document {gbif_id} sent with {sorted(UNINDEXED_FIELDS & source.keys())}, "
                 "indexConfig.sourceEnabled is false")

    log(f"  index {index} ({'own' if own else 'default'}), _source "
        f"{'disabled' if disabled else 'enabled'}, indexConfig.sourceEnabled {str(enabled).lower()}, "
        f"{min(len(gbif_ids), ES_SAMPLE_SIZE)} documents checked")
    return index


# ---------------------------------------------------------------------------- checks

def wait_for_load(state, version, started_ms, timeout_min):
    """Waits until the index has the records of the version and the load is committed"""
    dataset = state["dataset"]
    expected = len(id_range(state["records"], version))
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        reasons = [r for s, r in crawls(dataset) if s >= started_ms / 1000 - 60 and r]
        if reasons and reasons[0] != "NORMAL":
            fail(f"v{version} crawl finished with {reasons[0]}, nothing new was indexed")
        count = index_count(dataset)
        found = manifests(dataset)
        committed = (
            "current" in found
            and found["current"] >= started_ms
            and not {"_pending", "_previous", "_stale"} & found.keys()
        )
        steps = {t: f"{r}/{s}" for t, r, s in index_steps(dataset, started_ms / 1000)}
        log(f"  index {count}/{expected}, manifests {sorted(found)} committed={committed}, "
            f"INTERPRETED_TO_INDEX {steps.get('INTERPRETED_TO_INDEX', '-')}")
        if count == expected and committed:
            return steps
        time.sleep(POLL_SECONDS)
    fail(f"v{version} not indexed and committed after {timeout_min} min. Check the indexing logs "
         "(`kubectl logs -n lab deploy/occurrence-indexing-standalone -c occurrence-indexing-standalone`, "
         "or the Airflow/Spark driver logs for DISTRIBUTED) for 'Loaded'/'Removed'")


def run_step(state, version, args):
    dataset, records = state["dataset"], state["records"]
    first, last, count = versions(records)[version]
    ids = list(id_range(records, version))
    log(f"=== v{version}: test-{first}..test-{last} ({len(ids)} records), individualCount={count}")
    url = f"{args.archives_url.rstrip('/')}/v{version}.zip"
    ensure_newer_than_last_crawl(state, url, version, args.before_crawl)
    set_endpoint(state, url)
    started_ms = int(time.time() * 1000)
    state["lastCrawl"] = started_ms / 1000
    save_state(state)
    crawl(dataset)

    steps = wait_for_load(state, version, started_ms, args.timeout_min)
    log(f"  steps: {steps}")

    # the records loaded, sampled; the gbifIds are kept to check them once removed
    loaded = sample(ids)
    gbif_ids = lookup_gbif_ids(dataset, loaded)
    not_indexed = [occurrence_id(i) for i in loaded if occurrence_id(i) not in gbif_ids]
    if not_indexed:
        fail(f"{len(not_indexed)} sampled records not in the index: {not_indexed[:10]}")
    state.setdefault("gbifIds", {}).update(gbif_ids)
    save_state(state)

    expected = {gbif_ids[occurrence_id(i)] for i in loaded}
    attempt, removed, keys = check_records(state, ids, expected, count)
    previous = state.get("attempt")
    if previous is not None and int(attempt) <= int(previous):
        fail(f"Rows weren't rewritten: attempt {attempt}, previous load {previous}")

    state["index"] = check_index(dataset, expected, len(ids))
    state["attempt"] = attempt
    state["version"] = version
    save_state(state)

    threshold = bulk_load_threshold()
    log(f"OK v{version}: {len(ids)} records in the index, "
        f"manifest {'not counted (pip install pyarrow)' if keys is None else f'{keys} keys'}, "
        f"{len(expected)} sampled rows from attempt {attempt}, "
        f"{removed} sampled removed records absent, individualCount={count}, "
        f"{'bulk load' if len(ids) > threshold else 'Puts'} (bulkLoadIfRecordsMoreThan {threshold}), "
        f"runner {steps.get('INTERPRETED_TO_INDEX', '?')}")
    print_manifests(dataset)


def check_records(state, ids, expected, count):
    """Checks the rows of the sampled gbifIds in HBase (present from a single attempt, the removed
    records absent), the manifest and the staging directories. Returns the attempt of the rows,
    the sampled removed records and the manifest keys"""
    dataset = state["dataset"]
    current = {occurrence_id(i) for i in ids}
    removed = {g for o, g in state["gbifIds"].items() if o not in current}
    attempts = hbase_attempts(sorted(expected | removed))

    missing = sorted(g for g in expected if attempts[g] is None)
    if missing:
        fail(f"{len(missing)} of {len(expected)} sampled records missing from {TABLE}: {missing[:10]}")
    left = sorted(g for g in removed if attempts[g] is not None)
    if left:
        fail(f"{len(left)} of {len(removed)} removed records still in {TABLE}: {left[:10]}")

    loaded_attempts = {attempts[g] for g in expected}
    if len(loaded_attempts) != 1:
        fail(f"Rows of the dataset come from several attempts: {loaded_attempts}")
    attempt = loaded_attempts.pop()

    one = min(expected)
    if not re.search(rf"individualCount\W+{count}\b", hbase_interpreted(one)):
        fail(f"d:interpreted of {one} doesn't have individualCount {count}")

    keys = manifest_keys(dataset)
    if keys is not None and keys != len(ids):
        fail(f"Current manifest has {keys} keys, {len(ids)} expected")

    work = f"{OUTPUT_PATH}/{dataset}/{attempt}"
    leftovers = [d for d in [HFILE_DIR, f"{HFILE_DIR}-staging", "records-manifests"]
                 if hdfs_status(f"{work}/{d}")]
    if leftovers:
        fail(f"Staging directories left in {work}: {leftovers}")
    return attempt, len(removed), keys


def cmd_source(_args):
    """Checks the documents of the dataset against indexConfig.sourceEnabled, without crawling"""
    state = load_state()
    dataset = state.get("dataset") or fail(f"No dataset in {state_file}")
    version = state.get("version") or fail(f"Run the loads first: python3 {sys.argv[0]} run")
    current = {occurrence_id(i) for i in id_range(state["records"], version)}
    gbif_ids = {g for o, g in state["gbifIds"].items() if o in current}
    index = check_index(dataset, gbif_ids, len(current))
    log(f"OK source: {len(current)} documents in {index}")


def cmd_run(args):
    state = load_state()
    if not state.get("dataset"):
        if not args.org or not args.installation:
            fail("--org and --installation are needed to register the dataset")
        state = {"dataset": register_dataset(args.org, args.installation, args.records),
                 "records": args.records}
        save_state(state)
    else:
        state.setdefault("records", args.records)
        if state["records"] != args.records:
            fail(f"{state_file} is a test of {state['records']} records, use another --state")
        log(f"Continuing with dataset {state['dataset']} from {state_file}")
    for version in sorted(versions(args.records)):
        if version >= args.from_step:
            run_step(state, version, args)
    log(f"All loads OK. Next: python3 {sys.argv[0]} --state {state_file} delete")


def cmd_delete(args):
    state = load_state()
    dataset = state.get("dataset") or fail(f"No dataset in {state_file}")
    gbif_ids = sorted(set(state.get("gbifIds", {}).values()))
    http("DELETE", f"{API}/dataset/{dataset}", auth=True)
    log(f"Deleted dataset {dataset} in the registry, waiting for the dataset deleter")

    deadline = time.time() + args.timeout_min * 60
    while time.time() < deadline:
        found = manifests(dataset)
        rows = sum(a is not None for a in hbase_attempts(gbif_ids).values()) if gbif_ids else 0
        docs = sum(es_indices(dataset).values())
        log(f"  manifests {sorted(found)}, sampled rows left {rows}/{len(gbif_ids)}, documents {docs}")
        if not found and rows == 0 and docs == 0:
            log("OK delete: no documents, sampled rows or manifests left")
            return
        time.sleep(POLL_SECONDS)
    fail("Dataset not removed. Check that a DeleteDatasetOccurrencesMessage reached "
         "`kubectl logs -n lab deploy/occurrence-dataset-deleter -c occurrence-dataset-deleter`")


def cmd_status(_args):
    state = load_state()
    dataset = state.get("dataset") or fail(f"No dataset in {state_file}")
    log(f"Dataset {dataset}, {state.get('records')} records, last attempt checked {state.get('attempt')}")
    log(f"  index: {index_count(dataset)} records")
    keys = manifest_keys(dataset)
    if keys is not None:
        log(f"  current manifest: {keys} keys")
    gbif_ids = sorted(set(state.get("gbifIds", {}).values()))
    if gbif_ids:
        by_attempt = {}
        for a in hbase_attempts(gbif_ids).values():
            by_attempt[a] = by_attempt.get(a, 0) + 1
        log(f"  {TABLE}, sampled gbifIds by attempt (None = no row): {by_attempt}")
    for index, docs in es_indices(dataset).items():
        log(f"  {index}: {docs} documents, _source "
            f"{'disabled' if es_source_disabled(index) else 'enabled'}")
    log(f"  indexConfig.sourceEnabled {str(source_enabled()).lower()}")
    print_manifests(dataset)


def main():
    global state_file, es_url_override
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state", default=state_file, help=f"state file, default {state_file}")
    parser.add_argument("--es-url", help="Elasticsearch, default the first elastic.esHosts of the config map")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="build the test archives")
    build.add_argument("--records", type=int, default=100, help="records of v1, default 100")
    run = sub.add_parser("run", help="register the dataset (once) and run the loads")
    run.add_argument("--records", type=int, default=100, help="records of v1, as built")
    run.add_argument("--org", help="publishingOrganizationKey, to register the dataset")
    run.add_argument("--installation", help="installationKey, to register the dataset")
    run.add_argument("--archives-url", required=True, help="URL where v1.zip, v2.zip, v3.zip are")
    run.add_argument("--from-step", type=int, default=1, choices=[1, 2, 3])
    run.add_argument("--timeout-min", type=int, default=60, help="per load, default 60")
    run.add_argument(
        "--before-crawl",
        help="shell command run before each crawl to refresh the archive's Last-Modified, {version} "
        'is replaced, e.g. "ssh labs.gbif.org touch public_html/test-archives/v{version}.zip"',
    )
    delete = sub.add_parser("delete", help="delete the dataset and check its records are removed")
    delete.add_argument("--timeout-min", type=int, default=60)
    sub.add_parser("source", help="check the documents of the dataset against indexConfig.sourceEnabled")
    sub.add_parser("status", help="print the state of the dataset")
    args = parser.parse_args()
    state_file = args.state
    es_url_override = args.es_url

    {"build": lambda a: build_archives(a.records), "run": cmd_run, "source": cmd_source, "delete": cmd_delete,
     "status": cmd_status}[args.command](args)


if __name__ == "__main__":
    main()
