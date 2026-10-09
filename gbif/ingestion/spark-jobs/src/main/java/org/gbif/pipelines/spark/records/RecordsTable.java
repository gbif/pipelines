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
  private static final String LOAD_JSON = "__json";

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
    Dataset<Row> prepared = prepare(spark, documents, datasetAttempts);
    if (count <= config.getBulkLoadIfRecordsMoreThan()) {
      writePuts(prepared);
    } else {
      bulkLoad(prepared, fileSystem, workingDirectory);
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

  private void bulkLoad(Dataset<Row> prepared, FileSystem fileSystem, String workingDirectory)
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
      writeHFiles(prepared, type.converter(), loadConf, table, regionLocator, hfilePath);
      BulkLoadHFiles.create(loadConf).bulkLoad(tableName, hfilePath);
      fileSystem.delete(hfilePath, true);
      fileSystem.delete(stagingPath, true);
    }
  }

  private static void writeHFiles(
      Dataset<Row> prepared,
      RecordsConverter converter,
      Configuration hbaseConf,
      Table table,
      RegionLocator regionLocator,
      Path hfilePath)
      throws IOException {

    JavaPairRDD<Tuple2<String, String>, String> cells =
        prepared
            .javaRDD()
            .flatMapToPair(
                row -> {
                  String datasetKey = row.getAs(LOAD_DATASET_KEY);
                  String attempt = String.valueOf((Integer) row.getAs(LOAD_ATTEMPT));
                  RecordsConverter.ApiRecord record = converter.convert(row.getAs(LOAD_JSON));
                  String rowKey = record.rowKey();
                  List<Tuple2<Tuple2<String, String>, String>> result = new ArrayList<>(4);
                  result.add(new Tuple2<>(new Tuple2<>(rowKey, ATTEMPT_COLUMN), attempt));
                  result.add(new Tuple2<>(new Tuple2<>(rowKey, DATASET_KEY_COLUMN), datasetKey));
                  result.add(
                      new Tuple2<>(new Tuple2<>(rowKey, INTERPRETED_COLUMN), record.interpreted()));
                  result.add(
                      new Tuple2<>(new Tuple2<>(rowKey, VERBATIM_COLUMN), record.verbatim()));
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
              String column = cell._1._2;
              KeyValue kv =
                  new KeyValue(
                      row,
                      Bytes.toBytes(family(column)),
                      Bytes.toBytes(column),
                      Bytes.toBytes(cell._2));
              return new Tuple2<>(new ImmutableBytesWritable(row), kv);
            })
        .saveAsNewAPIHadoopFile(
            hfilePath.toString(),
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            jobConf);
  }

  /** Sends each row to the partition of the HBase region holding it, so HFiles align to regions */
  static class RegionPartitioner extends Partitioner {

    @Serial private static final long serialVersionUID = 1L;

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

  /** Orders cells by row key, family and column, as HFiles require */
  static class CellComparator implements Comparator<Tuple2<String, String>>, Serializable {

    @Serial private static final long serialVersionUID = 1L;

    @Override
    public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
      int byRow = o1._1.compareTo(o2._1);
      if (byRow != 0) {
        return byRow;
      }
      int byFamily = family(o1._2).compareTo(family(o2._2));
      return byFamily != 0 ? byFamily : o1._2.compareTo(o2._2);
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
