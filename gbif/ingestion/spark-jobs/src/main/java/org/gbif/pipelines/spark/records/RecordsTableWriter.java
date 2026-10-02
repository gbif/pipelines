package org.gbif.pipelines.spark.records;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.gbif.pipelines.spark.records.RecordsTableKey.ATTEMPT_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.COLUMN_FAMILY;
import static org.gbif.pipelines.spark.records.RecordsTableKey.DATASET_KEY_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.INTERPRETED_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.VERBATIM_COLUMN;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;
import scala.Tuple2;

/**
 * Writes the API representation of records to HBase and removes the records that are no longer part
 * of their dataset.
 *
 * <p>The keys loaded for each dataset are kept in a manifest, outside the dataset attempt
 * directories, at {@code <manifestPath>/<type>/datasetKey=<datasetKey>}. A load compares the
 * manifests of the datasets it loads with the new keys to find the removed records, and replaces
 * the manifests only once {@link RecordsLoad#commit()} is called, after the index has been updated.
 * A failed run therefore leaves the previous manifests in place and the next run repeats the
 * deletes.
 *
 * <p>A single dataset (incremental indexing) and many datasets (full index build) are loaded the
 * same way, in one bulk load.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RecordsTableWriter {

  /** Column holding the dataset key in the indexed documents */
  public static final String DOCUMENT_DATASET_KEY = "datasetKey";

  private static final String ROW_KEY = "rowKey";
  private static final String MANIFEST_PARTITION = "datasetKey";
  private static final String PENDING = "_pending";
  private static final String PREVIOUS = "_previous";
  private static final String STAGING = "-staging";

  private static final String LOAD_DATASET_KEY = "__datasetKey";
  private static final String LOAD_ATTEMPT = "__attempt";
  private static final String LOAD_JSON = "__json";

  /** Kinds of records, each in its own table */
  public enum RecordType {
    OCCURRENCE,
    EVENT;

    RecordsConverter converter() {
      return this == OCCURRENCE ? RecordsConverter.forOccurrences() : RecordsConverter.forEvents();
    }

    String table(RecordsTableConfig config) {
      String table = this == OCCURRENCE ? config.getOccurrenceTable() : config.getEventTable();
      return Objects.requireNonNull(table, "No HBase records table configured for " + this);
    }

    String manifestDirectory() {
      return name().toLowerCase();
    }
  }

  /** Result of a load, the manifests of the loaded datasets are replaced on commit */
  @Getter
  public static class RecordsLoad {
    private final long loaded;
    private final long removed;
    private final transient FileSystem fileSystem;
    private final transient RecordsTableConfig config;
    private final transient RecordType type;
    private final transient List<String> datasetKeys;

    private RecordsLoad(
        long loaded,
        long removed,
        FileSystem fileSystem,
        RecordsTableConfig config,
        RecordType type,
        List<String> datasetKeys) {
      this.loaded = loaded;
      this.removed = removed;
      this.fileSystem = fileSystem;
      this.config = config;
      this.type = type;
      this.datasetKeys = datasetKeys;
    }

    /** Replaces the manifests of the loaded datasets with the keys of this load */
    public void commit() throws IOException {
      for (String datasetKey : datasetKeys) {
        Path manifest = manifestPath(config, type, datasetKey, "");
        Path pending = manifestPath(config, type, datasetKey, PENDING);
        Path previous = manifestPath(config, type, datasetKey, PREVIOUS);

        fileSystem.delete(previous, true);
        if (!fileSystem.exists(pending)) {
          // no records left in the dataset, they were all removed
          fileSystem.delete(manifest, true);
          continue;
        }
        if (fileSystem.exists(manifest) && !fileSystem.rename(manifest, previous)) {
          throw new IOException("Can't move manifest " + manifest + " to " + previous);
        }
        fileSystem.mkdirs(manifest.getParent());
        if (!fileSystem.rename(pending, manifest)) {
          throw new IOException("Can't move manifest " + pending + " to " + manifest);
        }
        fileSystem.delete(previous, true);
      }
    }
  }

  /** HBase configuration for the cluster described in the pipelines configuration */
  public static Configuration hbaseConfiguration(PipelinesConfig config) {
    Configuration conf = HBaseConfiguration.create();
    if (config.getHbaseSiteConfig() != null) {
      conf.addResource(new Path(config.getHbaseSiteConfig()));
    }
    if (config.getHdfsSiteConfig() != null && config.getCoreSiteConfig() != null) {
      conf.addResource(new Path(config.getHdfsSiteConfig()));
      conf.addResource(new Path(config.getCoreSiteConfig()));
    }
    return conf;
  }

  /**
   * Loads the records of a single dataset, see {@link #load(SparkSession, FileSystem,
   * PipelinesConfig, Configuration, RecordType, Map, Dataset, String)}
   */
  public static RecordsLoad load(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      Configuration hbaseConf,
      RecordType type,
      String datasetKey,
      int attempt,
      Dataset<Row> documents)
      throws IOException {
    String workingDirectory =
        String.format("%s/%s/%d", config.getOutputPath(), datasetKey, attempt);
    return load(
        spark,
        fileSystem,
        config,
        hbaseConf,
        type,
        Map.of(datasetKey, attempt),
        documents,
        workingDirectory);
  }

  /**
   * Loads the records of the given datasets and deletes the ones loaded by previous runs that are
   * no longer present. Call {@link RecordsLoad#commit()} once the index has been updated.
   *
   * @param datasetAttempts the datasets to load and the attempt each one comes from. A dataset
   *     without documents has all its records removed.
   * @param documents the documents indexed in Elasticsearch, with a {@value DOCUMENT_DATASET_KEY}
   *     column
   * @param workingDirectory directory where the HFiles are staged
   */
  public static RecordsLoad load(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      Configuration hbaseConf,
      RecordType type,
      Map<String, Integer> datasetAttempts,
      Dataset<Row> documents,
      String workingDirectory)
      throws IOException {

    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    RecordsConverter converter = type.converter();
    TableName tableName = TableName.valueOf(type.table(tableConfig));
    Path hfilePath = new Path(workingDirectory + "/" + tableConfig.getHfilePath());
    Path stagingPath = new Path(workingDirectory + "/" + tableConfig.getHfilePath() + STAGING);
    List<String> datasetKeys =
        datasetAttempts.keySet().stream().sorted().collect(Collectors.toList());

    Dataset<Row> datasetDocuments =
        documents.where(col(DOCUMENT_DATASET_KEY).isin(datasetKeys.toArray()));
    Dataset<Row> prepared = prepare(spark, datasetDocuments, datasetAttempts);

    // the new keys are written first as pending manifests, and read back from there
    for (String datasetKey : datasetKeys) {
      // stale pending manifests of a failed run must not be committed
      fileSystem.delete(manifestPath(tableConfig, type, datasetKey, PENDING), true);
    }
    rowKeys(datasetDocuments, converter)
        .write()
        .mode(SaveMode.Append)
        .partitionBy(MANIFEST_PARTITION)
        .parquet(manifestRoot(tableConfig, type, PENDING).toString());

    List<String> pendingManifests = new ArrayList<>();
    for (String datasetKey : datasetKeys) {
      Path pending = manifestPath(tableConfig, type, datasetKey, PENDING);
      if (fileSystem.exists(pending)) {
        pendingManifests.add(pending.toString());
      }
    }
    Dataset<String> rowKeys =
        pendingManifests.isEmpty()
            ? spark.emptyDataset(Encoders.STRING())
            : spark
                .read()
                .parquet(pendingManifests.toArray(new String[0]))
                .select(ROW_KEY)
                .as(Encoders.STRING());
    long loaded = rowKeys.count();

    if (loaded > 0) {
      try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
          Admin admin = connection.getAdmin();
          Table table = connection.getTable(tableName);
          RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
        // HFileOutputFormat2 stages its partitions file there, keep it with the job outputs
        Configuration loadConf = new Configuration(hbaseConf);
        loadConf.set("hbase.fs.tmp.dir", stagingPath.toString());

        fileSystem.delete(hfilePath, true);
        writeHFiles(prepared, converter, loadConf, table, regionLocator, hfilePath);
        new LoadIncrementalHFiles(loadConf).doBulkLoad(hfilePath, admin, table, regionLocator);
        fileSystem.delete(hfilePath, true);
        fileSystem.delete(stagingPath, true);
      }
    }

    long removed = 0;
    List<String> previousManifests = existingManifests(fileSystem, tableConfig, type, datasetKeys);
    if (!previousManifests.isEmpty()) {
      Dataset<String> removedKeys =
          spark
              .read()
              .parquet(previousManifests.toArray(new String[0]))
              .select(ROW_KEY)
              .as(Encoders.STRING())
              .except(rowKeys);
      removed = delete(removedKeys, hbaseConf, tableName, tableConfig.getDeleteBatchSize());
    }

    log.info(
        "Loaded {} {} records of {} datasets into {}, removed {}",
        loaded,
        type,
        datasetKeys.size(),
        tableName,
        removed);
    return new RecordsLoad(loaded, removed, fileSystem, tableConfig, type, datasetKeys);
  }

  /** Deletes all records of a dataset and its manifest */
  public static long deleteDataset(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      Configuration hbaseConf,
      RecordType type,
      String datasetKey)
      throws IOException {

    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    List<String> manifests = existingManifests(fileSystem, tableConfig, type, List.of(datasetKey));
    if (manifests.isEmpty()) {
      log.warn("No {} records manifest for dataset {}, nothing to delete", type, datasetKey);
      return 0;
    }

    Dataset<String> keys =
        spark.read().parquet(manifests.get(0)).select(ROW_KEY).as(Encoders.STRING());
    long deleted =
        delete(
            keys,
            hbaseConf,
            TableName.valueOf(type.table(tableConfig)),
            tableConfig.getDeleteBatchSize());

    for (String state : new String[] {"", PENDING, PREVIOUS}) {
      fileSystem.delete(manifestPath(tableConfig, type, datasetKey, state), true);
    }
    log.info("Deleted {} {} records of dataset {}", deleted, type, datasetKey);
    return deleted;
  }

  static Path manifestPath(RecordsTableConfig config, RecordType type, String datasetKey) {
    return manifestPath(config, type, datasetKey, "");
  }

  private static Path manifestPath(
      RecordsTableConfig config, RecordType type, String datasetKey, String state) {
    return new Path(manifestRoot(config, type, state), MANIFEST_PARTITION + "=" + datasetKey);
  }

  private static Path manifestRoot(RecordsTableConfig config, RecordType type, String state) {
    String root =
        Objects.requireNonNull(config.getManifestPath(), "No records manifest path configured");
    return new Path(root + "/" + type.manifestDirectory() + state);
  }

  /** The manifest of each dataset, or the previous one when a commit was interrupted */
  private static List<String> existingManifests(
      FileSystem fileSystem, RecordsTableConfig config, RecordType type, List<String> datasetKeys)
      throws IOException {
    List<String> result = new ArrayList<>();
    for (String datasetKey : datasetKeys) {
      Path manifest = manifestPath(config, type, datasetKey, "");
      Path previous = manifestPath(config, type, datasetKey, PREVIOUS);
      if (fileSystem.exists(manifest)) {
        result.add(manifest.toString());
      } else if (fileSystem.exists(previous)) {
        result.add(previous.toString());
      }
    }
    return result;
  }

  /** Documents as JSON, with the dataset and attempt they belong to */
  private static Dataset<Row> prepare(
      SparkSession spark, Dataset<Row> documents, Map<String, Integer> datasetAttempts) {
    List<Row> attempts =
        datasetAttempts.entrySet().stream()
            .map(e -> RowFactory.create(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    Dataset<Row> attemptsDf =
        spark.createDataFrame(
            attempts,
            new StructType()
                .add(LOAD_DATASET_KEY, DataTypes.StringType)
                .add(LOAD_ATTEMPT, DataTypes.IntegerType));

    Column[] columns =
        Arrays.stream(documents.columns()).map(c -> col("`" + c + "`")).toArray(Column[]::new);
    return documents
        .select(
            col(DOCUMENT_DATASET_KEY).as(LOAD_DATASET_KEY), to_json(struct(columns)).as(LOAD_JSON))
        .join(broadcast(attemptsDf), LOAD_DATASET_KEY);
  }

  /** Row keys of the documents, with their dataset */
  private static Dataset<Row> rowKeys(Dataset<Row> documents, RecordsConverter converter) {
    String keyField = converter.keyField();
    boolean occurrence = converter instanceof OccurrenceRecordsConverter;
    return documents
        .select(col(DOCUMENT_DATASET_KEY), col(keyField))
        .map(
            (MapFunction<Row, Tuple2<String, String>>)
                row -> {
                  Object key = row.get(1);
                  if (key == null) {
                    throw new IllegalArgumentException("Document without " + keyField);
                  }
                  String rowKey =
                      occurrence
                          ? RecordsTableKey.occurrenceRowKey(Long.parseLong(key.toString()))
                          : RecordsTableKey.eventRowKey(key.toString());
                  return new Tuple2<>(row.getString(0), rowKey);
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF(MANIFEST_PARTITION, ROW_KEY);
  }

  private static void writeHFiles(
      Dataset<Row> prepared,
      RecordsConverter converter,
      Configuration hbaseConf,
      Table table,
      RegionLocator regionLocator,
      Path hfilePath)
      throws IOException {

    byte[] family = Bytes.toBytes(COLUMN_FAMILY);

    JavaPairRDD<Tuple2<String, String>, String> cells =
        prepared
            .javaRDD()
            .flatMapToPair(
                row -> {
                  String datasetKey = row.getAs(LOAD_DATASET_KEY);
                  String attempt = String.valueOf((Integer) row.getAs(LOAD_ATTEMPT));
                  RecordsConverter.ApiRecord record = converter.convert(row.getAs(LOAD_JSON));
                  String rowKey = record.getRowKey();
                  List<Tuple2<Tuple2<String, String>, String>> result = new ArrayList<>(4);
                  result.add(new Tuple2<>(new Tuple2<>(rowKey, ATTEMPT_COLUMN), attempt));
                  result.add(new Tuple2<>(new Tuple2<>(rowKey, DATASET_KEY_COLUMN), datasetKey));
                  result.add(
                      new Tuple2<>(
                          new Tuple2<>(rowKey, INTERPRETED_COLUMN), record.getInterpreted()));
                  result.add(
                      new Tuple2<>(new Tuple2<>(rowKey, VERBATIM_COLUMN), record.getVerbatim()));
                  return result.iterator();
                })
            .repartitionAndSortWithinPartitions(
                new RegionPartitioner(regionLocator.getStartKeys()), new CellComparator());

    // carries the table settings (compression, bloom filter, block size) to the HFiles
    Job job = Job.getInstance(hbaseConf);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    Configuration jobConf = job.getConfiguration();

    cells
        .mapToPair(
            cell -> {
              byte[] row = Bytes.toBytes(cell._1._1);
              KeyValue kv =
                  new KeyValue(row, family, Bytes.toBytes(cell._1._2), Bytes.toBytes(cell._2));
              return new Tuple2<>(new ImmutableBytesWritable(row), kv);
            })
        .saveAsNewAPIHadoopFile(
            hfilePath.toString(),
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            jobConf);
  }

  private static long delete(
      Dataset<String> rowKeys, Configuration hbaseConf, TableName tableName, int batchSize) {
    SerializableConfiguration conf = new SerializableConfiguration(hbaseConf);
    String table = tableName.getNameAsString();
    return rowKeys
        .javaRDD()
        .mapPartitions(
            keys -> {
              long count = 0;
              BufferedMutatorParams params =
                  new BufferedMutatorParams(TableName.valueOf(table))
                      .writeBufferSize((long) batchSize * 128);
              try (Connection connection = ConnectionFactory.createConnection(conf.get());
                  BufferedMutator mutator = connection.getBufferedMutator(params)) {
                while (keys.hasNext()) {
                  mutator.mutate(new Delete(Bytes.toBytes(keys.next())));
                  count++;
                }
              }
              return List.of(count).iterator();
            })
        .fold(0L, Long::sum);
  }

  /** Sends each row to the partition of the HBase region holding it, so HFiles align to regions */
  static class RegionPartitioner extends Partitioner {
    private static final long serialVersionUID = 1L;

    private final String[] startKeys;

    RegionPartitioner(byte[][] regionStartKeys) {
      startKeys = Arrays.stream(regionStartKeys).map(Bytes::toString).toArray(String[]::new);
      Arrays.sort(startKeys);
    }

    @Override
    public int numPartitions() {
      return startKeys.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int getPartition(Object key) {
      String rowKey = ((Tuple2<String, String>) key)._1;
      int index = Arrays.binarySearch(startKeys, rowKey);
      // not a start key: the region is the one before the insertion point
      return index >= 0 ? index : Math.max(0, -index - 2);
    }
  }

  /** Orders cells by row key and column, as HFiles require */
  static class CellComparator implements Comparator<Tuple2<String, String>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
      int byRow = o1._1.compareTo(o2._1);
      return byRow != 0 ? byRow : o1._2.compareTo(o2._2);
    }
  }

  /** Hadoop configurations aren't serializable, this ships one to the executors */
  static class SerializableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient Configuration configuration;

    SerializableConfiguration(Configuration configuration) {
      this.configuration = configuration;
    }

    Configuration get() {
      return configuration;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      configuration.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      configuration = new Configuration(false);
      configuration.readFields(in);
    }
  }
}
