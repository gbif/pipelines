# HBase records tables

Occurrence and event records are served by the API from HBase. Elasticsearch only searches: its
indices have the `_source` disabled and return document ids, which the web services use to fetch
the records from these tables.

| Table              | Holds             | Row key                    | API endpoints                                   |
|--------------------|-------------------|----------------------------|-------------------------------------------------|
| occurrence records | occurrences       | salted `gbifId`            | `occurrence/{key}`, `occurrence/{key}/verbatim` |
| event records      | events            | `internalId`               | `event/{key}`                                   |

Both tables are written by `IndexingPipeline` (one dataset) and `FullIndexBuildPipeline` (all
datasets) before the documents are indexed, and the records of a dataset are removed by
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

The tables are compressed with ZSTD, and the codec has to be available on both sides:

- HBase, to read the HFiles. Check it on a node with the HBase configuration:

  ```shell
  hbase org.apache.hadoop.hbase.util.CompressionTest hdfs:///tmp/compression-test zstd
  ```

- The Spark executors, which write the HFiles with the compression of the column family
  (`HFileOutputFormat2.configureIncrementalLoad`). They need the native `libzstd` of Hadoop on the
  YARN nodes, or `org.apache.hbase:hbase-compression-zstd` on the job classpath. Without it the job
  fails writing the HFiles, before anything is loaded.

In the HBase shell:

```ruby
# Occurrences: 100 regions, one per salt bucket ("00:" ... "99:")
create 'prod_occurrence',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'ZSTD', DATA_BLOCK_ENCODING => 'FAST_DIFF',
   BLOOMFILTER => 'ROW', BLOCKSIZE => '32768'},
  {NUMREGIONS => 100, SPLITALGO => 'DecimalStringSplit'}

# Events: 16 regions, one per first hex character of the SHA-1
create 'prod_event',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'ZSTD', DATA_BLOCK_ENCODING => 'FAST_DIFF',
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
  deleteBatchSize: 1000
```

`hbaseSiteConfig`, `coreSiteConfig` and `hdfsSiteConfig` from the pipelines configuration are used
to connect to HBase.

### Manifests

The keys loaded for each dataset are kept in
`<manifestPath>/<occurrence|event>/datasetKey=<datasetKey>`. When a dataset is indexed again, the
keys of its previous manifest that aren't in the new load are deleted from HBase, and the manifest
is replaced, once the index no longer returns them (`_pending` and `_previous` directories hold the
manifests during a run). The keys loaded by a failed run are kept in a `_stale` directory, and the
next commit deletes the ones that aren't in its load too.

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
