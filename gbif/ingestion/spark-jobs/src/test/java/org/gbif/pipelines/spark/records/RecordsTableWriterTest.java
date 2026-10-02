package org.gbif.pipelines.spark.records;

import static org.gbif.pipelines.spark.records.RecordsTableKey.ATTEMPT_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.COLUMN_FAMILY;
import static org.gbif.pipelines.spark.records.RecordsTableKey.DATASET_KEY_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.INTERPRETED_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.VERBATIM_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
    assertEquals(0, first.getRemoved());
    first.commit();

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
    assertEquals(1, second.getRemoved());

    assertTrue(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(20L)).isEmpty());
    assertEquals(
        "2", value(get(OCCURRENCE_TABLE, RecordsTableKey.occurrenceRowKey(1L)), ATTEMPT_COLUMN));

    // until committed, the previous manifest is kept so a failed run repeats the deletes
    Path manifest =
        RecordsTableWriter.manifestPath(
            config.getRecordsTableConfig(), RecordType.OCCURRENCE, OCCURRENCE_DATASET);
    assertEquals(3, manifestKeys(manifest).size());
    second.commit();
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
    Path manifestA = RecordsTableWriter.manifestPath(tableConfig, RecordType.OCCURRENCE, datasetA);
    Path manifestB = RecordsTableWriter.manifestPath(tableConfig, RecordType.OCCURRENCE, datasetB);
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
