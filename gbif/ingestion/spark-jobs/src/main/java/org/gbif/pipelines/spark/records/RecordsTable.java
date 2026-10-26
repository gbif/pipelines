package org.gbif.pipelines.spark.records;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.gbif.pipelines.spark.records.RecordsTableKey.ATTEMPT_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.DATASET_KEY_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.INTERPRETED_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.VERBATIM_COLUMN;
import static org.gbif.pipelines.spark.records.RecordsTableKey.family;
import static org.gbif.pipelines.spark.records.RecordsTableWriter.DOCUMENT_DATASET_KEY;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordType;
import scala.Tuple2;

/**
 * Writes and deletes the rows of a records table. Loads of more than {@link
 * RecordsTableConfig#getBulkLoadIfRecordsMoreThan()} records are written as HFiles and bulk loaded,
 * smaller ones with Puts: the HFiles of a small load would add a small store file to every region
 * it touches, to compact later.
 */
@Slf4j
class RecordsTable {

  private static final String STAGING = "-staging";

  private static final String LOAD_DATASET_KEY = "__datasetKey";
  private static final String LOAD_ATTEMPT = "__attempt";
  static final String LOAD_JSON = "__json";

  /** Columns of a row in the order HFiles require: by family, then column */
  static final List<String> HFILE_COLUMNS =
      Stream.of(ATTEMPT_COLUMN, DATASET_KEY_COLUMN, INTERPRETED_COLUMN, VERBATIM_COLUMN)
          .sorted(
              Comparator.comparing(RecordsTableKey::family)
                  .thenComparing(Comparator.naturalOrder()))
          .toList();

  private final Configuration hbaseConf;
  private final RecordsTableConfig config;
  private final RecordType type;
  private final TableName tableName;

  RecordsTable(Configuration hbaseConf, RecordsTableConfig config, RecordType type) {
    this.hbaseConf = hbaseConf;
    this.config = config;
    this.type = type;
    this.tableName = TableName.valueOf(type.table(config));
  }

  TableName name() {
    return tableName;
  }

  /**
   * Writes the records of the documents
   *
   * @param datasetAttempts the attempt each dataset of the documents comes from
   * @param count number of documents, to choose between Puts and a bulk load
   * @param workingDirectory directory where the HFiles are staged
   */
  void write(
      SparkSession spark,
      FileSystem fileSystem,
      Dataset<Row> documents,
      Map<String, Integer> datasetAttempts,
      long count,
      String workingDirectory)
      throws IOException {
    Dataset<Row> prepared =
        prepare(spark, documents, datasetAttempts, type.converter().excludedFields());
    if (count <= config.getBulkLoadIfRecordsMoreThan()) {
      writePuts(prepared);
    } else {
      bulkLoad(prepared, fileSystem, workingDirectory, count);
    }
  }

  /** @return the number of rows deleted */
  long delete(Dataset<String> rowKeys) {
    SerializableConfiguration conf = new SerializableConfiguration(hbaseConf);
    String table = tableName.getNameAsString();
    long bufferSize = (long) config.getDeleteBatchSize() * 128;
    return rowKeys
        .javaRDD()
        .mapPartitions(
            keys -> {
              long count = 0;
              BufferedMutatorParams params =
                  new BufferedMutatorParams(TableName.valueOf(table)).writeBufferSize(bufferSize);
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

  /** Empties the table, keeping its regions and settings */
  void truncate() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        throw new IOException("Records table " + tableName + " doesn't exist");
      }
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      // the table is enabled again once truncated
      admin.truncateTable(tableName, true);
    }
  }

  /**
   * Documents as JSON, with the dataset and attempt they belong to. The excluded top-level fields
   * are dropped before the JSON is written, as the converter would parse and drop them: "all"
   * repeats every value of an occurrence.
   */
  static Dataset<Row> prepare(
      SparkSession spark,
      Dataset<Row> documents,
      Map<String, Integer> datasetAttempts,
      List<String> excludedFields) {
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
        Arrays.stream(documents.columns())
            .filter(c -> !excludedFields.contains(c))
            .map(c -> col("`" + c + "`"))
            .toArray(Column[]::new);
    return documents
        .select(
            col(DOCUMENT_DATASET_KEY).as(LOAD_DATASET_KEY), to_json(struct(columns)).as(LOAD_JSON))
        .join(broadcast(attemptsDf), LOAD_DATASET_KEY);
  }

  private void writePuts(Dataset<Row> prepared) {
    SerializableConfiguration conf = new SerializableConfiguration(hbaseConf);
    String table = tableName.getNameAsString();
    RecordsConverter converter = type.converter();
    prepared
        .javaRDD()
        .foreachPartition(
            rows -> {
              try (Connection connection = ConnectionFactory.createConnection(conf.get());
                  BufferedMutator mutator =
                      connection.getBufferedMutator(TableName.valueOf(table))) {
                while (rows.hasNext()) {
                  Row row = rows.next();
                  RecordsConverter.ApiRecord record = converter.convert(row.getAs(LOAD_JSON));
                  // both families in the same Put, the row is written atomically
                  Put put = new Put(Bytes.toBytes(record.rowKey()));
                  addColumn(put, ATTEMPT_COLUMN, String.valueOf((Integer) row.getAs(LOAD_ATTEMPT)));
                  addColumn(put, DATASET_KEY_COLUMN, row.getAs(LOAD_DATASET_KEY));
                  addColumn(put, INTERPRETED_COLUMN, record.interpreted());
                  addColumn(put, VERBATIM_COLUMN, record.verbatim());
                  mutator.mutate(put);
                }
              }
            });
  }

  private static void addColumn(Put put, String column, String value) {
    put.addColumn(Bytes.toBytes(family(column)), Bytes.toBytes(column), Bytes.toBytes(value));
  }

  private void bulkLoad(
      Dataset<Row> prepared, FileSystem fileSystem, String workingDirectory, long count)
      throws IOException {
    Path hfilePath = new Path(workingDirectory + "/" + config.getHfilePath());
    Path stagingPath = new Path(workingDirectory + "/" + config.getHfilePath() + STAGING);

    try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Table table = connection.getTable(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
      // HFileOutputFormat2 stages its partitions file there, keep it with the job outputs
      Configuration loadConf = new Configuration(hbaseConf);
      loadConf.set("hbase.fs.tmp.dir", stagingPath.toString());
      // the tables are created pre-split beforehand, fail instead of creating a default one
      loadConf.set(BulkLoadHFiles.CREATE_TABLE_CONF_KEY, "no");

      fileSystem.delete(hfilePath, true);
      int partitions =
          hfilePartitions(
              prepared.rdd().getNumPartitions(), count, config.getBulkLoadRecordsPerPartition());
      writeHFiles(
          prepared, type.converter(), loadConf, table, regionLocator, hfilePath, partitions);
      BulkLoadHFiles.create(loadConf).bulkLoad(tableName, hfilePath);
      fileSystem.delete(hfilePath, true);
      fileSystem.delete(stagingPath, true);
    }
  }

  /**
   * Partitions to write the HFiles with: enough to keep each one under {@code recordsPerPartition}
   * records, and no fewer than the input has
   */
  static int hfilePartitions(int inputPartitions, long count, long recordsPerPartition) {
    long needed = (count + recordsPerPartition - 1) / recordsPerPartition;
    return (int) Math.min(Integer.MAX_VALUE, Math.max(inputPartitions, needed));
  }

  /**
   * Writes the records as HFiles of at most {@code partitions} partitions. A record is shuffled and
   * sorted as one entry, its row key and the values in {@link #HFILE_COLUMNS} order, and expanded
   * into cells after the sort: the cells of a row are then already in the order HFiles need.
   */
  private static void writeHFiles(
      Dataset<Row> prepared,
      RecordsConverter converter,
      Configuration hbaseConf,
      Table table,
      RegionLocator regionLocator,
      Path hfilePath,
      int partitions)
      throws IOException {

    JavaPairRDD<String, String[]> records =
        prepared
            .javaRDD()
            .mapToPair(
                row -> {
                  RecordsConverter.ApiRecord record = converter.convert(row.getAs(LOAD_JSON));
                  Map<String, String> values =
                      Map.of(
                          ATTEMPT_COLUMN, String.valueOf((Integer) row.getAs(LOAD_ATTEMPT)),
                          DATASET_KEY_COLUMN, row.getAs(LOAD_DATASET_KEY),
                          INTERPRETED_COLUMN, record.interpreted(),
                          VERBATIM_COLUMN, record.verbatim());
                  return new Tuple2<>(
                      record.rowKey(),
                      HFILE_COLUMNS.stream().map(values::get).toArray(String[]::new));
                })
            .repartitionAndSortWithinPartitions(
                new RegionPartitioner(regionLocator.getStartKeys(), partitions));

    // carries the table settings (compression, bloom filter, block size) to the HFiles
    Job job = Job.getInstance(hbaseConf);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    Configuration jobConf = job.getConfiguration();

    records
        .flatMapToPair(
            record -> {
              byte[] row = Bytes.toBytes(record._1);
              ImmutableBytesWritable key = new ImmutableBytesWritable(row);
              List<Tuple2<ImmutableBytesWritable, KeyValue>> cells =
                  new ArrayList<>(HFILE_COLUMNS.size());
              for (int i = 0; i < HFILE_COLUMNS.size(); i++) {
                String column = HFILE_COLUMNS.get(i);
                KeyValue kv =
                    new KeyValue(
                        row,
                        Bytes.toBytes(family(column)),
                        Bytes.toBytes(column),
                        Bytes.toBytes(record._2[i]));
                cells.add(new Tuple2<>(key, kv));
              }
              return cells.iterator();
            })
        .saveAsNewAPIHadoopFile(
            hfilePath.toString(),
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            jobConf);
  }

  /**
   * Sends each row to a partition of the HBase region holding it, so HFiles align to regions. A
   * region is spread over several partitions, by row key hash, so a large load isn't shuffled into
   * one partition per region: a full build would push a hundredth of all the records through each
   * one. Bulk loading accepts several HFiles per region, as long as each one is sorted and within
   * the region.
   */
  static class RegionPartitioner extends Partitioner {

    @Serial private static final long serialVersionUID = 1L;

    private final String[] startKeys;
    private final int partitionsPerRegion;

    /** @param targetPartitions approximate number of partitions, at least one per region */
    RegionPartitioner(byte[][] regionStartKeys, int targetPartitions) {
      startKeys = Arrays.stream(regionStartKeys).map(Bytes::toString).toArray(String[]::new);
      Arrays.sort(startKeys);
      partitionsPerRegion =
          Math.max(1, (targetPartitions + startKeys.length - 1) / startKeys.length);
    }

    @Override
    public int numPartitions() {
      return startKeys.length * partitionsPerRegion;
    }

    @Override
    public int getPartition(Object key) {
      String rowKey = (String) key;
      int index = Arrays.binarySearch(startKeys, rowKey);
      // not a start key: the region is the one before the insertion point
      int region = index >= 0 ? index : Math.max(0, -index - 2);
      // all the cells of a row go to the same partition
      return region * partitionsPerRegion + Math.floorMod(rowKey.hashCode(), partitionsPerRegion);
    }
  }

  /** Hadoop configurations aren't serializable, this ships one to the executors */
  static class SerializableConfiguration implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private transient Configuration configuration;

    SerializableConfiguration(Configuration configuration) {
      this.configuration = configuration;
    }

    Configuration get() {
      return configuration;
    }

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
      configuration.write(out);
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      configuration = new Configuration(false);
      configuration.readFields(in);
    }
  }
}
