package org.gbif.pipelines.spark.records;

import static org.gbif.pipelines.spark.records.RecordsTableKey.ATTEMPT_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.COLUMN_FAMILY;
import static org.gbif.pipelines.spark.records.RecordsTableKey.DATASET_KEY_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.INTERPRETED_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.VERBATIM_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;
import org.gbif.pipelines.spark.HbaseServer;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordType;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordsLoad;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class RecordsTableWriterTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private static final String OCCURRENCE_TABLE = "test_records_occurrence";
  private static final String EVENT_TABLE = "test_records_event";
  private static final String OCCURRENCE_DATASET = "7683cc47-cb13-4bad-9614-387c66aa8df0";
  private static final String EVENT_DATASET = "8d5fe649-f85e-43cc-a19c-2a9979a741ac";
  private static final String EVENT_INTERNAL_ID = "cbf64c0df611eae2fc0c2a3234f0eeac8f423071";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static SparkSession spark;
  private static FileSystem fileSystem;
  private static Configuration hbaseConf;
  private static PipelinesConfig config;

  @BeforeClass
  public static void setUp() throws Exception {
    spark = SparkTestSession.create();
    fileSystem = FileSystem.getLocal(spark.sparkContext().hadoopConfiguration());
    // as in production, the job directories are on the default filesystem of the HBase client
    hbaseConf = new Configuration(HBASE_SERVER.getConnection().getConfiguration());
    hbaseConf.set("fs.defaultFS", "file:///");

    // several regions, so records are partitioned and bulk loaded per region
    createTable(OCCURRENCE_TABLE, "10:", "50:");
    createTable(EVENT_TABLE, "8");

    String root = "file://" + Files.createTempDirectory("records-table").toAbsolutePath();
    RecordsTableConfig tableConfig = new RecordsTableConfig();
    tableConfig.setOccurrenceTable(OCCURRENCE_TABLE);
    tableConfig.setEventTable(EVENT_TABLE);
    tableConfig.setManifestPath(root + "/manifests");
    // the fixtures are small, bulk load them anyway, see smallLoadsAreWrittenWithPuts
    tableConfig.setBulkLoadIfRecordsMoreThan(0);

    config = new PipelinesConfig();
    config.setOutputPath(root + "/data");
    config.setRecordsTableConfig(tableConfig);
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void occurrencesAreLoadedReplacedAndDeleted() throws Exception {
    // attempt 1: three records spread over the three regions
    RecordsLoad first = load(1, occurrences(1L, 20L, 75L));
    assertEquals(3, first.getLoaded());
    assertEquals(0, first.commit());

    for (long key : new long[] {1L, 20L, 75L}) {
      Result row = get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(key));
      assertEquals(OCCURRENCE_DATASET, value(row, DATASET_KEY_COLUMN));
      assertEquals("1", value(row, ATTEMPT_COLUMN));
      assertEquals(key, MAPPER.readTree(value(row, INTERPRETED_COLUMN)).path("key").asLong());
      assertEquals(key, MAPPER.readTree(value(row, VERBATIM_COLUMN)).path("key").asLong());
    }

    // attempt 2: record 20 is no longer in the dataset
    RecordsLoad second = load(2, occurrences(1L, 75L));
    assertEquals(2, second.getLoaded());
    assertEquals(
        "2", value(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(1L)), ATTEMPT_COLUMN));

    // until committed, the index can still return record 20, so it's kept, and so is the previous
    // manifest, so a failed run repeats the deletes
    assertFalse(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(20L)).isEmpty());
    Path manifest =
        RecordsManifests.path(
            config.getRecordsTableConfig(), RecordType.OCCURRENCE, OCCURRENCE_DATASET);
    assertEquals(3, manifestKeys(manifest).size());

    assertEquals(1, second.commit());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(20L)).isEmpty());
    assertEquals(2, manifestKeys(manifest).size());

    // the dataset is deleted
    long deleted =
        RecordsTableWriter.deleteDataset(
            spark, fileSystem, config, hbaseConf, RecordType.OCCURRENCE, OCCURRENCE_DATASET);
    assertEquals(2, deleted);
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(1L)).isEmpty());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(75L)).isEmpty());
    assertFalse(fileSystem.exists(manifest));
  }

  /** Full index build: many datasets in a single load, each with its own manifest */
  @Test
  public void datasetsAreLoadedTogether() throws Exception {
    String datasetA = "a0000000-0000-0000-0000-000000000001";
    String datasetB = "b0000000-0000-0000-0000-000000000002";
    String workingDirectory = config.getOutputPath() + "/rebuild";

    RecordsLoad first =
        RecordsTableWriter.load(
            spark,
            fileSystem,
            config,
            hbaseConf,
            RecordType.OCCURRENCE,
            Map.of(datasetA, 3, datasetB, 4),
            occurrences(datasetA, 101L, 102L).unionByName(occurrences(datasetB, 201L)),
            workingDirectory);
    first.commit();

    assertEquals(3, first.getLoaded());
    Result a = get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(101L));
    assertEquals(datasetA, value(a, DATASET_KEY_COLUMN));
    assertEquals("3", value(a, ATTEMPT_COLUMN));
    Result b = get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(201L));
    assertEquals(datasetB, value(b, DATASET_KEY_COLUMN));
    assertEquals("4", value(b, ATTEMPT_COLUMN));

    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    Path manifestA = RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, datasetA);
    Path manifestB = RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, datasetB);
    assertEquals(2, manifestKeys(manifestA).size());
    assertEquals(1, manifestKeys(manifestB).size());

    // dataset A has no records anymore, B is unchanged
    RecordsLoad second =
        RecordsTableWriter.load(
            spark,
            fileSystem,
            config,
            hbaseConf,
            RecordType.OCCURRENCE,
            Map.of(datasetA, 5, datasetB, 5),
            occurrences(datasetB, 201L),
            workingDirectory);
    second.commit();

    assertEquals(1, second.getLoaded());
    assertEquals(2, second.getRemoved());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(101L)).isEmpty());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(102L)).isEmpty());
    assertEquals(
        "5", value(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(201L)), ATTEMPT_COLUMN));
    assertFalse(fileSystem.exists(manifestA));
    assertEquals(1, manifestKeys(manifestB).size());
  }

  @Test
  public void eventsAreLoadedByInternalId() throws Exception {
    Dataset<Row> events = documents(List.of(readResource("/records/event-es-source.json")));

    RecordsLoad load =
        RecordsTableWriter.load(
            spark, fileSystem, config, hbaseConf, RecordType.EVENT, EVENT_DATASET, 1, events);
    load.commit();

    assertEquals(1, load.getLoaded());
    Result row = get(EVENT_TABLE, EVENT_INTERNAL_ID);
    assertEquals(EVENT_DATASET, value(row, DATASET_KEY_COLUMN));
    assertEquals(
        "EVT-001", MAPPER.readTree(value(row, INTERPRETED_COLUMN)).path("eventID").asText());
  }

  @Test
  public void datasetWithoutManifestDeletesNothing() throws Exception {
    assertEquals(
        0,
        RecordsTableWriter.deleteDataset(
            spark, fileSystem, config, hbaseConf, RecordType.OCCURRENCE, "unknown-dataset"));
  }

  @Test
  public void previousManifestIsKeptUntilReplaced() throws Exception {
    String dataset = "3f2c1e6a-9d4b-4c8e-a1f7-5b0e2d9c7a13";
    Path manifest =
        RecordsManifests.path(config.getRecordsTableConfig(), RecordType.OCCURRENCE, dataset);
    Path previous =
        RecordsManifests.path(
            config.getRecordsTableConfig(),
            RecordType.OCCURRENCE,
            dataset,
            RecordsManifests.PREVIOUS);

    load(fileSystem, dataset, 1, occurrences(dataset, 2L, 30L)).commit();

    // a commit interrupted once the manifest was moved away leaves only the previous manifest
    assertTrue(fileSystem.rename(manifest, previous));

    // the next commit fails to promote its manifest
    FileSystem failingPromotion =
        new FilterFileSystem(fileSystem) {
          @Override
          public boolean rename(Path src, Path dst) throws IOException {
            return !src.getParent().getName().endsWith(RecordsManifests.PENDING)
                && super.rename(src, dst);
          }
        };
    RecordsLoad second = load(failingPromotion, dataset, 2, occurrences(dataset, 2L));
    assertThrows(IOException.class, second::commit);
    assertEquals(1, second.getRemoved());
    assertTrue(fileSystem.exists(previous));

    // so the following run still knows the keys of the dataset
    assertEquals(1, load(fileSystem, dataset, 3, occurrences(dataset, 2L)).commit());
    assertEquals(List.of(RecordsTableKey.occurrenceRowKey(2L)), manifestKeys(manifest));
    assertFalse(fileSystem.exists(previous));
  }

  @Test
  public void keysOfFailedRunsAreRemoved() throws Exception {
    String dataset = "9a7e4b21-6c3d-4f05-8e1a-2d4c6b8f0e57";
    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    Path manifest = RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, dataset);
    Path stale =
        RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, dataset, RecordsManifests.STALE);

    load(fileSystem, dataset, 1, occurrences(dataset, 5L, 40L)).commit();

    // attempt 2 fails after loading 41 and 42, its keys aren't in the manifest
    load(fileSystem, dataset, 2, occurrences(dataset, 5L, 41L, 42L));
    assertFalse(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(41L)).isEmpty());

    // attempt 3 no longer has 40 (in the manifest) nor 41 (only loaded by the failed run)
    RecordsLoad third = load(fileSystem, dataset, 3, occurrences(dataset, 5L, 42L));
    assertTrue(fileSystem.exists(stale));
    assertEquals(2, third.commit());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(40L)).isEmpty());
    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(41L)).isEmpty());
    assertFalse(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(42L)).isEmpty());
    assertFalse(fileSystem.exists(stale));
    assertEquals(2, manifestKeys(manifest).size());

    // deleting the dataset also deletes the keys of a run that wasn't committed
    load(fileSystem, dataset, 4, occurrences(dataset, 5L, 43L));
    assertEquals(
        3,
        RecordsTableWriter.deleteDataset(
            spark, fileSystem, config, hbaseConf, RecordType.OCCURRENCE, dataset));
    for (long key : new long[] {5L, 42L, 43L}) {
      assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(key)).isEmpty());
    }
  }

  /** Incremental indexing loads several datasets at the same time, sharing the manifest root */
  @Test
  public void concurrentLoadsKeepTheirManifests() throws Exception {
    int datasets = 4;
    ExecutorService executor = Executors.newFixedThreadPool(datasets);
    try {
      List<Future<Long>> commits = new ArrayList<>();
      for (int i = 0; i < datasets; i++) {
        String dataset = "5d6e7f80-0000-4000-8000-00000000000" + i;
        long first = 1000L + i * 100;
        commits.add(
            executor.submit(
                () ->
                    load(fileSystem, dataset, 1, occurrences(dataset, first, first + 1, first + 2))
                        .commit()));
      }
      for (Future<Long> commit : commits) {
        assertEquals(0L, (long) commit.get());
      }
    } finally {
      executor.shutdown();
    }

    for (int i = 0; i < datasets; i++) {
      String dataset = "5d6e7f80-0000-4000-8000-00000000000" + i;
      long first = 1000L + i * 100;
      Path manifest =
          RecordsManifests.path(config.getRecordsTableConfig(), RecordType.OCCURRENCE, dataset);
      List<String> expected = new ArrayList<>();
      for (long key = first; key < first + 3; key++) {
        expected.add(RecordsTableKey.occurrenceRowKey(key));
      }
      List<String> keys = new ArrayList<>(manifestKeys(manifest));
      Collections.sort(keys);
      Collections.sort(expected);
      assertEquals(dataset, expected, keys);
    }
  }

  /** As in production: manifests, HFiles and the HBase root on HDFS */
  @Test
  public void manifestsOnHdfs() throws Exception {
    FileSystem hdfs = HBASE_SERVER.getDfs();
    String root = hdfs.getUri() + "/records-table";
    RecordsTableConfig tableConfig = new RecordsTableConfig();
    tableConfig.setOccurrenceTable(OCCURRENCE_TABLE);
    tableConfig.setEventTable(EVENT_TABLE);
    tableConfig.setManifestPath(root + "/manifests");
    // the fixtures are small, bulk load them anyway, see smallLoadsAreWrittenWithPuts
    tableConfig.setBulkLoadIfRecordsMoreThan(0);
    PipelinesConfig hdfsConfig = new PipelinesConfig();
    hdfsConfig.setOutputPath(root + "/data");
    hdfsConfig.setRecordsTableConfig(tableConfig);
    Configuration hdfsHbaseConf =
        new Configuration(HBASE_SERVER.getConnection().getConfiguration());

    String dataset = "c4e8a1f2-7b3d-4e6a-9f05-1d2b3c4e5f60";
    Path manifest = RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, dataset);

    // loaded, replaced, then a failed run whose keys are removed by the next one
    assertEquals(0, hdfsLoad(hdfs, hdfsConfig, hdfsHbaseConf, dataset, 1, 6L, 60L).commit());
    assertEquals(1, hdfsLoad(hdfs, hdfsConfig, hdfsHbaseConf, dataset, 2, 6L).commit());
    hdfsLoad(hdfs, hdfsConfig, hdfsHbaseConf, dataset, 3, 6L, 61L);
    assertEquals(1, hdfsLoad(hdfs, hdfsConfig, hdfsHbaseConf, dataset, 4, 6L).commit());

    assertTrue(hdfs.exists(manifest));
    assertEquals(List.of(RecordsTableKey.occurrenceRowKey(6L)), manifestKeys(manifest));
    for (long key : new long[] {60L, 61L}) {
      assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(key)).isEmpty());
    }
    assertEquals(
        1,
        RecordsTableWriter.deleteDataset(
            spark, hdfs, hdfsConfig, hdfsHbaseConf, RecordType.OCCURRENCE, dataset));
  }

  @Test
  public void smallLoadsAreWrittenWithPuts() throws Exception {
    String table = "test_records_puts";
    String dataset = "5b2f6a71-0c3e-4f8d-8e2a-6d4c1b9a3e57";
    createTable(table, "10:", "50:");
    PipelinesConfig putsConfig = copyConfig(table);
    putsConfig.getRecordsTableConfig().setBulkLoadIfRecordsMoreThan(10);
    Path hfiles = new Path(config.getOutputPath() + "/" + dataset + "/1/records-hfile");

    RecordsLoad first =
        RecordsTableWriter.load(
            spark,
            fileSystem,
            putsConfig,
            hbaseConf,
            RecordType.OCCURRENCE,
            dataset,
            1,
            occurrences(dataset, 3L, 33L, 83L));
    assertEquals(3, first.getLoaded());
    assertEquals(0, first.commit());
    assertFalse(fileSystem.exists(hfiles));

    for (long key : new long[] {3L, 33L, 83L}) {
      Result row = get(table, RecordsTableKey.occurrenceRowKey(key));
      assertEquals(dataset, value(row, DATASET_KEY_COLUMN));
      assertEquals("1", value(row, ATTEMPT_COLUMN));
      assertEquals(key, MAPPER.readTree(value(row, INTERPRETED_COLUMN)).path("key").asLong());
      assertEquals(key, MAPPER.readTree(value(row, VERBATIM_COLUMN)).path("key").asLong());
    }

    // replaced and removed as with a bulk load
    assertEquals(
        1,
        RecordsTableWriter.load(
                spark,
                fileSystem,
                putsConfig,
                hbaseConf,
                RecordType.OCCURRENCE,
                dataset,
                2,
                occurrences(dataset, 3L, 83L))
            .commit());
    assertEquals("2", value(get(table, RecordsTableKey.occurrenceRowKey(3L)), ATTEMPT_COLUMN));
    assertTrue(get(table, RecordsTableKey.occurrenceRowKey(33L)).isEmpty());
  }

  @Test
  public void truncateEmptiesTheTableKeepingItsRegions() throws Exception {
    String table = "test_records_truncate";
    String dataset = "1d0e1a3c-5f2b-4d2a-9a51-2f3c6b9e7d10";
    createTable(table, "10:", "50:");
    PipelinesConfig truncateConfig = copyConfig(table);
    RecordsTableConfig tableConfig = truncateConfig.getRecordsTableConfig();

    RecordsTableWriter.load(
            spark,
            fileSystem,
            truncateConfig,
            hbaseConf,
            RecordType.OCCURRENCE,
            dataset,
            1,
            occurrences(dataset, 1L, 20L, 75L))
        .commit();
    // a failed run leaves a pending manifest, kept as stale by the next load
    RecordsTableWriter.load(
        spark,
        fileSystem,
        truncateConfig,
        hbaseConf,
        RecordType.OCCURRENCE,
        dataset,
        2,
        occurrences(dataset, 1L, 30L));
    RecordsTableWriter.load(
        spark,
        fileSystem,
        truncateConfig,
        hbaseConf,
        RecordType.OCCURRENCE,
        dataset,
        3,
        occurrences(dataset, 1L));

    RecordsTableWriter.truncate(fileSystem, truncateConfig, hbaseConf, RecordType.OCCURRENCE);

    for (long key : new long[] {1L, 20L, 30L, 75L}) {
      assertTrue(get(table, RecordsTableKey.occurrenceRowKey(key)).isEmpty());
    }
    try (RegionLocator locator =
        HBASE_SERVER.getConnection().getRegionLocator(TableName.valueOf(table))) {
      assertEquals(3, locator.getStartKeys().length);
    }
    for (String state :
        new String[] {
          "", RecordsManifests.PENDING, RecordsManifests.PREVIOUS, RecordsManifests.STALE
        }) {
      Path manifest = RecordsManifests.path(tableConfig, RecordType.OCCURRENCE, dataset, state);
      assertFalse(fileSystem.exists(manifest.getParent()));
    }

    // built again from scratch, nothing to remove
    RecordsLoad rebuilt =
        RecordsTableWriter.load(
            spark,
            fileSystem,
            truncateConfig,
            hbaseConf,
            RecordType.OCCURRENCE,
            dataset,
            4,
            occurrences(dataset, 5L));
    assertEquals(0, rebuilt.commit());
    assertEquals("4", value(get(table, RecordsTableKey.occurrenceRowKey(5L)), ATTEMPT_COLUMN));
  }

  @Test
  public void truncateRefusesTheKeygenTable() throws Exception {
    PipelinesConfig keygenConfig = copyConfig(HbaseServer.CFG.getOccurrenceTable());
    KeygenConfig keygen = new KeygenConfig();
    keygen.setOccurrenceTable(HbaseServer.CFG.getOccurrenceTable());
    keygenConfig.setKeygen(keygen);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecordsTableWriter.truncate(
                fileSystem, keygenConfig, hbaseConf, RecordType.OCCURRENCE));
    try (Admin admin = HBASE_SERVER.getConnection().getAdmin()) {
      assertTrue(admin.isTableEnabled(TableName.valueOf(HbaseServer.CFG.getOccurrenceTable())));
    }
  }

  /** The test configuration with another occurrence table and its own manifests */
  private static PipelinesConfig copyConfig(String occurrenceTable) throws Exception {
    RecordsTableConfig tableConfig = new RecordsTableConfig();
    tableConfig.setOccurrenceTable(occurrenceTable);
    tableConfig.setEventTable(EVENT_TABLE);
    tableConfig.setManifestPath(
        "file://" + Files.createTempDirectory("records-manifests").toAbsolutePath());

    PipelinesConfig copy = new PipelinesConfig();
    copy.setOutputPath(config.getOutputPath());
    copy.setRecordsTableConfig(tableConfig);
    return copy;
  }

  private static RecordsLoad hdfsLoad(
      FileSystem hdfs,
      PipelinesConfig hdfsConfig,
      Configuration hdfsHbaseConf,
      String datasetKey,
      int attempt,
      long... keys)
      throws Exception {
    return RecordsTableWriter.load(
        spark,
        hdfs,
        hdfsConfig,
        hdfsHbaseConf,
        RecordType.OCCURRENCE,
        datasetKey,
        attempt,
        occurrences(datasetKey, keys));
  }

  private static RecordsLoad load(int attempt, Dataset<Row> documents) throws Exception {
    return RecordsTableWriter.load(
        spark,
        fileSystem,
        config,
        hbaseConf,
        RecordType.OCCURRENCE,
        OCCURRENCE_DATASET,
        attempt,
        documents);
  }

  private static RecordsLoad load(
      FileSystem fs, String datasetKey, int attempt, Dataset<Row> documents) throws Exception {
    return RecordsTableWriter.load(
        spark, fs, config, hbaseConf, RecordType.OCCURRENCE, datasetKey, attempt, documents);
  }

  /** Copies of the occurrence fixture with the given keys */
  private static Dataset<Row> occurrences(long... keys) throws Exception {
    return occurrences(OCCURRENCE_DATASET, keys);
  }

  /** Copies of the occurrence fixture in the given dataset with the given keys */
  private static Dataset<Row> occurrences(String datasetKey, long... keys) throws Exception {
    String fixture = readResource("/records/occurrence-es-source.json");
    List<String> documents = new ArrayList<>();
    for (long key : keys) {
      ObjectNode document = (ObjectNode) MAPPER.readTree(fixture);
      document.put("gbifId", key);
      document.put("datasetKey", datasetKey);
      documents.add(MAPPER.writeValueAsString(document));
    }
    return documents(documents);
  }

  private static Dataset<Row> documents(List<String> json) {
    return spark.read().json(spark.createDataset(json, Encoders.STRING()));
  }

  private static List<String> manifestKeys(Path manifest) {
    return spark
        .read()
        .parquet(manifest.toString())
        .select("rowKey")
        .as(Encoders.STRING())
        .collectAsList();
  }

  private static void createTable(String name, String... splits) throws Exception {
    byte[][] splitKeys = new byte[splits.length][];
    for (int i = 0; i < splits.length; i++) {
      splitKeys[i] = Bytes.toBytes(splits[i]);
    }
    try (Admin admin = HBASE_SERVER.getConnection().getAdmin()) {
      admin.createTable(
          TableDescriptorBuilder.newBuilder(TableName.valueOf(name))
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY))
              .build(),
          splitKeys);
    }
  }

  private static Result get(String table, String rowKey) throws Exception {
    try (Table t = HBASE_SERVER.getConnection().getTable(TableName.valueOf(table))) {
      return t.get(new Get(Bytes.toBytes(rowKey)));
    }
  }

  private static String value(Result row, String column) {
    byte[] value = row.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(column));
    assertNotNull("missing " + column, value);
    return Bytes.toString(value);
  }

  private static String readResource(String path) throws Exception {
    try (InputStream in = RecordsTableWriterTest.class.getResourceAsStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
