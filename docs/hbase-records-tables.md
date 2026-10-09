# HBase records tables

Occurrence and event records are served by the API from HBase. Elasticsearch only searches: its
indices have the `_source` disabled and return document ids, which the web services use to fetch
the records from these tables.

Disabling the `_source` is controlled by `indexConfig.sourceEnabled` (default `true`), so the tables
can be loaded and the API switched to them without a full re-index:

1. With `sourceEnabled: true`, the indices keep their `_source` (as defined by the schema files)
   and receive the full documents, while the tables are written as well.
2. Once the API has been reading from the tables in production for a while, set
   `sourceEnabled: false`. New indices are created with `"_source": {"enabled": false}` and the
   fields mapped with `"enabled": false` (`verbatim`, `multimediaItems`) are no longer sent.
   Existing indices keep their mappings until they are rebuilt (e.g. by `FullIndexBuildPipeline`).

```yaml
indexConfig:
  sourceEnabled: false
```

| Table              | Holds             | Row key                    | API endpoints                                   |
|--------------------|-------------------|----------------------------|-------------------------------------------------|
| occurrence records | occurrences       | salted `gbifId`            | `occurrence/{key}`, `occurrence/{key}/verbatim` |
| event records      | events            | `internalId`               | `event/{key}`                                   |

Both tables are written by `IndexingPipeline` (one dataset), `DatasetsIndexBuildPipeline` (a list
of datasets), `FullIndexBuildPipeline` (all datasets) and `FullRecordsTableBuildPipeline` (all datasets, tables only) before the documents are indexed, and the records of a dataset are removed by
`DatasetDeleteCallback`. See `gbif/ingestion/spark-jobs/src/main/java/org/gbif/pipelines/spark/records`.

## Layout

Both tables have a single column family, `o`, with these columns:

| Column          | Value                                                                       |
|-----------------|-----------------------------------------------------------------------------|
| `o:interpreted` | JSON of the API `Occurrence` (or `Event`), as returned by the web services  |
| `o:verbatim`    | JSON of the API `VerbatimOccurrence`                                        |
| `o:datasetKey`  | Dataset the record belongs to                                               |
| `o:attempt`     | Crawl attempt the record comes from                                         |

The JSON is serialized with the same Jackson configuration as the web services (non-null fields,
ISO-8601 dates, GBIF mixins), so it can be returned as is or read into the API model classes.

### Occurrence row keys

```
<gbifId % 100, zero-padded to 2 digits>:<gbifId>

gbifId 1234567  ->  67:1234567
gbifId 4000002  ->  02:4000002
```

`gbifId`s are assigned sequentially, so without the salt a new dataset would be written to, and
read from, a single region. The salt is derived from the key itself: a reader computes the row key
from the `gbifId` and fetches the record with a single `Get`. This is the same salting as the
fragments table (`Keygen.getSaltedKey`, `RecordsTableKey.occurrenceRowKey`).

### Event row keys

The `internalId` as is: a SHA-1 of the dataset key and the record id, 40 lowercase hex characters
(e.g. `cbf64c0df611eae2fc0c2a3234f0eeac8f423071`). It is already evenly distributed, so it isn't
salted.

## Creating the tables

Create the tables pre-split, so every region gets a share of the keys from the first load. The
writer partitions the HFiles along the regions of the table, whatever they are. The tables must
exist before the first load: the bulk load is configured not to create them.

They must be dedicated tables, never the keygen `occurrenceTable` (or any other table): the writer
bulk loads into them and removes records with whole-row deletes, which would also remove any other
cells stored under the same row key.

The tables are compressed with SNAPPY until the cluster is updated to support ZSTD. The codec has
to be available on both sides:

- HBase, to read the HFiles. Check it on a node with the HBase configuration:

  ```shell
  hbase org.apache.hadoop.hbase.util.CompressionTest hdfs:///tmp/compression-test snappy
  ```

- The Spark executors, which write the HFiles with the compression of the column family
  (`HFileOutputFormat2.configureIncrementalLoad`). Without the codec the job fails writing the
  HFiles, before anything is loaded.

In the HBase shell:

```ruby
# Occurrences: 100 regions, one per salt bucket ("00:" ... "99:")
create 'lab_occurrence',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF',
   BLOOMFILTER => 'ROW', BLOCKSIZE => '32768'},
  {NUMREGIONS => 100, SPLITALGO => 'DecimalStringSplit'}

# Events: 16 regions, one per first hex character of the SHA-1
create 'lab_event',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF',
   BLOOMFILTER => 'ROW', BLOCKSIZE => '32768'},
  {NUMREGIONS => 16, SPLITALGO => 'HexStringSplit'}
```

- `NUMREGIONS`/`SPLITALGO`: `DecimalStringSplit` splits `00000000`-`99999999`, so the split points
  are `01000000`, `02000000`, ... `99000000`. A salted key such as `01:1234567` sorts between
  `01000000` and `02000000` (`:` sorts after `0`), so each salt bucket gets its own region.
  `HexStringSplit` splits at `10000000`, `20000000`, ... `f0000000`, matching the lowercase SHA-1s.
- `BLOOMFILTER => 'ROW'`: every dataset load adds HFiles to the regions, a `Get` skips the ones
  that don't hold the row.
- `BLOCKSIZE => '32768'`: reads are random `Get`s, smaller blocks than the 64KB default mean less
  data read and decompressed per record.
- `DATA_BLOCK_ENCODING => 'FAST_DIFF'`: the cells of a row repeat its key, the encoding stores the
  differences only.
- `VERSIONS => 1`: each load replaces the record, older versions aren't needed.

As the tables grow, HBase splits the regions further, and the writer follows whatever regions
exist. To start the event table with more regions, use e.g. `{NUMREGIONS => 256, SPLITALGO =>
'HexStringSplit'}`.

Check the tables:

```ruby
describe 'prod_occurrence'
list_regions 'prod_occurrence'
```

### Moving to ZSTD

ZSTD compresses the JSON of the records better than SNAPPY. Once the cluster supports it:

1. Check the codec on HBase (`CompressionTest ... zstd`) and on the executors: they need the native
   `libzstd` of Hadoop on the YARN nodes, or `org.apache.hbase:hbase-compression-zstd` on the job
   classpath.
2. Change the column family, it applies to the HFiles written from then on:

   ```ruby
   alter 'prod_occurrence', {NAME => 'o', COMPRESSION => 'ZSTD'}
   alter 'prod_event', {NAME => 'o', COMPRESSION => 'ZSTD'}
   ```

3. Rewrite the existing HFiles with a major compaction (or rebuild the tables):

   ```ruby
   major_compact 'prod_occurrence'
   major_compact 'prod_event'
   ```

## Configuration

```yaml
recordsTableConfig:
  occurrenceTable: prod_occurrence
  eventTable: prod_event
  # one manifest per dataset with the keys loaded, used to delete the records removed from a dataset.
  # Must be outside the dataset/attempt directories, which are cleaned up after each run.
  manifestPath: hdfs://ha-nn/data/ingest/records-manifests
  # optional
  hfilePath: records-hfile
  bulkLoadIfRecordsMoreThan: 50000
  deleteBatchSize: 1000
```

A load of more than `bulkLoadIfRecordsMoreThan` records (a big dataset, or a full build) is written
as HFiles and bulk loaded. Smaller ones, most incremental loads, are written with `Put`s: the
HFiles of a small load would add a small store file to every region it touches, to compact later.

`hbaseSiteConfig`, `coreSiteConfig` and `hdfsSiteConfig` from the pipelines configuration are used
to connect to HBase.

### Manifests

HBase can't list the rows of a dataset without scanning the whole table, so the row keys loaded for
each dataset are kept in a manifest, a parquet with one `rowKey` column. When a dataset is indexed
again, the keys in its old manifest that aren't in the new load are the records removed from the
dataset, and they are deleted from HBase (see `RecordsManifests` and `RecordsTableWriter`).

A manifest is in one of four states. The state is a suffix of the record type directory, not a
subdirectory:

```
<manifestPath>/
  occurrence/datasetKey=<key>/                    current: keys of the last committed load
  occurrence_pending/datasetKey=<key>/            keys of the running load, written before its records
  occurrence_previous/datasetKey=<key>/           the current manifest while a commit replaces it
  occurrence_stale/datasetKey=<key>/run=<millis>/ pending manifests of failed runs, one per run
  event/...                                       the same for events
```

A run has two steps:

1. **Load** (`RecordsTableWriter.load`), before indexing. A pending manifest left by an earlier run
   means that run failed before committing: it is moved to `_stale`. Then the keys of the new
   documents are written to `_pending` and the records are written to HBase. Nothing is deleted
   yet, so the old records are still served while the index still returns them.
2. **Commit** (`RecordsLoad.commit`), once the index no longer returns the removed keys. Deletes
   from HBase:

   ```
   (current, or previous if there is no current)  ∪  every stale manifest  −  pending
   ```

   and then replaces the manifests: `current` → `_previous`, `_pending` → `current`, and removes
   `_previous` and `_stale`. The manifests are moved after the deletes, so if the deletes fail the
   next run repeats them. Deleting a row that doesn't exist does nothing, so repeating them is safe.

#### Examples

Dataset `A`, indexed for the first time with records `1 2 3`:

| Step   | HBase       | current | pending | stale | Deleted |
|--------|-------------|---------|---------|-------|---------|
| load   | `1 2 3`     | –       | `1 2 3` | –     |         |
| commit | `1 2 3`     | `1 2 3` | –       | –     | nothing |

**Records removed.** `A` is indexed again with `2 3 4`, record `1` was removed from the dataset:

| Step   | HBase       | current | pending | stale | Deleted                    |
|--------|-------------|---------|---------|-------|----------------------------|
| load   | `1 2 3 4`   | `1 2 3` | `2 3 4` | –     |                            |
| commit | `2 3 4`     | `2 3 4` | –       | –     | `{1 2 3} − {2 3 4}` = `1`  |

Between the load and the commit, `1` is still in HBase: the old index can still return it, and the
API can still serve it.

**Failed run.** `A` (current `2 3 4`) is crawled with `2 3 4 5`, but indexing fails after the load,
so there is no commit. `5` is in HBase but in no committed manifest. The next run has `2 3 4`:

| Step           | HBase       | current | pending   | stale       | Deleted                                    |
|----------------|-------------|---------|-----------|-------------|--------------------------------------------|
| load (fails)   | `2 3 4 5`   | `2 3 4` | `2 3 4 5` | –           |                                            |
| next load      | `2 3 4 5`   | `2 3 4` | `2 3 4`   | `2 3 4 5`   |                                            |
| next commit    | `2 3 4`     | `2 3 4` | –         | –           | `{2 3 4} ∪ {2 3 4 5} − {2 3 4}` = `5`      |

Without the stale manifest, `5` would stay in HBase forever. Several failed runs in a row leave one
`run=<millis>` directory each, and the next commit takes all of them into account.

**Commit interrupted.** The job dies after moving `current` → `_previous`, but before moving
`_pending` → `current`. The deletes have already happened (`4` was removed). On disk there is
`_previous` (`2 3 4`), `_pending` (`2 3`) and no `current`. The next run, again with `2 3`, moves
`_pending` to `_stale` as for a failed run, and reads `_previous` as the last committed manifest.
Its commit deletes `{2 3 4} ∪ {2 3} − {2 3}` = `4` again, which does nothing, then moves its own
`_pending` to `current` and removes `_previous`.

**Dataset now empty.** `A` (current `2 3 4`) is indexed with no records. No pending manifest is
written, so the commit deletes `{2 3 4} − {}`, all of them, and removes every manifest of `A`.

**Dataset deleted** (`DatasetDeleteCallback`). After removing the dataset from Elasticsearch, it
deletes every key in any manifest of the dataset (current or previous, stale and pending), and
then removes the manifests in all four states.

#### What removes the manifests

The `datasetKey=<key>` directories are kept for as long as the dataset has records in HBase. They
are removed by:

| What                                         | Removes                                                     |
|----------------------------------------------|-------------------------------------------------------------|
| A commit                                     | `_previous` and `_stale`, plus `current` if the load was empty |
| `DatasetDeleteCallback` (dataset deleted)    | All four states of the dataset, after deleting its records  |
| `FullRecordsTableBuildPipeline --truncate`   | All manifests of the record type, after emptying the table  |
| Manually (building from scratch, see below)  | Everything under `manifestPath`                             |

A load first writes the keys to `<workingDirectory>/records-manifests`, then moves each dataset to
`_pending` and removes that directory.

Nothing else removes them. If a dataset is deleted and its delete message is never processed
successfully, its manifests and its records stay until the message is replayed or the table is
rebuilt with `--truncate`. A rebuild without `--truncate` only commits the datasets it loads, so it
doesn't remove them either. The empty `<type>_pending`, `_previous` and `_stale` root directories
stay after their datasets are committed. They take no space.

## Building from scratch

The tables and the indices are built together, with indexing stopped:

1. Stop the indexing (coordinator callbacks).
2. Empty the tables keeping their splits, and remove the manifests:

   ```ruby
   truncate_preserve 'prod_occurrence'
   truncate_preserve 'prod_event'
   ```

   ```shell
   hdfs dfs -rm -r -skipTrash /data/ingest/records-manifests
   ```

3. Run `FullIndexBuildPipeline` for occurrences and for events. It loads the tables, writes the
   manifests and builds the new indices.
4. Swap the aliases (or use `--switchOnSuccess`) and deploy the web services reading from HBase.
5. Restart the indexing.

### Re-indexing some datasets

`DatasetsIndexBuildPipeline` fixes a few datasets without a full build. Each dataset is indexed
from its last successful interpretation as when it's crawled (`IndexingPipeline`): its records are
loaded into the table, it's indexed into the live alias (its own index or the default one, by
size), and its previous documents, indices and removed records are deleted. The other datasets
aren't touched. A dataset that fails doesn't stop the others; the job fails at the end listing them.

```shell
DatasetsIndexBuildPipeline --config=pipelines.yaml --datasetType=OCCURRENCE \
  --datasetKeys=50c9509d-22c7-4a22-a47d-8c48425ef4a7,7e380070-f762-11e1-a439-00145eb45e9a
```

`FullIndexBuildPipeline` can't be used for this: `--switchOnSuccess` points the live alias to the
rebuilt indices only, which would hold just those datasets.

### Rebuilding the tables only

`FullRecordsTableBuildPipeline` rebuilds a table from the last successful interpretation of every
dataset (the `json` parquet, as `FullIndexBuildPipeline`), without touching Elasticsearch. It
loads all the datasets in one bulk load and writes their manifests.

```shell
# in place: empties the table (keeping its regions) and its manifests, then loads it
FullRecordsTableBuildPipeline --config=pipelines.yaml --datasetType=OCCURRENCE --truncate=true

# next to the live table: create the new table pre-split first, as above
FullRecordsTableBuildPipeline --config=pipelines.yaml --datasetType=SAMPLING_EVENT \
  --table=prod_event_20261007 \
  --manifestPath=hdfs://ha-nn/data/ingest/records-manifests-20261007
```

- In place, the API serves no records from the table until the load completes. Without
  `--truncate`, the records are replaced and the ones no longer in the datasets are deleted, but
  the records of datasets no longer interpreted (e.g. deleted ones) stay.
- Next to the live table, the manifests must be new too, as they describe the keys of one table.
  Once built, point `recordsTableConfig` (table and `manifestPath`) and the web services at the new
  table. Stop the indexing during the build, or the datasets indexed meanwhile are only in the old
  table.
- `truncate` refuses the keygen and fragments tables.

## Reading a record

```java
long gbifId = 1234567L;
String rowKey = String.format("%02d:%d", gbifId % 100, gbifId);  // RecordsTableKey.occurrenceRowKey

Get get = new Get(Bytes.toBytes(rowKey));
get.addColumn(Bytes.toBytes("o"), Bytes.toBytes("interpreted"));  // or "verbatim"
Result result = table.get(get);
String json = Bytes.toString(result.getValue(Bytes.toBytes("o"), Bytes.toBytes("interpreted")));
```

For a page of search results, send one `table.get(List<Get>)` with the keys returned by
Elasticsearch, keep the Elasticsearch order, and skip missing rows: a record can be deleted after
the search was answered.

## Testing on lab

`gbif/ingestion/spark-jobs/scripts/records_test.py` is an end-to-end test on lab with a dataset
registered for it. It crawls three versions of a generated archive (records added, removed and
rewritten) and after each load checks the index, the HBase rows, the manifests, and the
Elasticsearch documents against `indexConfig.sourceEnabled` (`verbatim` kept in the `_source`, or
not sent and the index of the dataset created with the `_source` disabled). It needs `kubectl`
access to the lab and test namespaces and Python 3; see its docstring (`--help`) for the steps.

```shell
python3 gbif/ingestion/spark-jobs/scripts/records_test.py build --records 200000
python3 gbif/ingestion/spark-jobs/scripts/records_test.py run --records 200000 --org <key> \
    --installation <key> --archives-url <url of the archives>
# set indexConfig.sourceEnabled: false, restart the indexing, and crawl v3 again
python3 gbif/ingestion/spark-jobs/scripts/records_test.py run --records 200000 --from-step 3 \
    --archives-url <url of the archives>
python3 gbif/ingestion/spark-jobs/scripts/records_test.py delete
```
