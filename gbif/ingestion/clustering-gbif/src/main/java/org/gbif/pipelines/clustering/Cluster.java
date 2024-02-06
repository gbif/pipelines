package org.gbif.pipelines.clustering;

import static org.gbif.pipelines.clustering.HashUtilities.recordHashes;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import lombok.Builder;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;
import scala.Tuple2;

/**
 * Regenerates the HBase and Hive tables for a data clustering run. This will read the occurrence
 * Hive table, generate HFiles for relationships and create intermediate tables in Hive for
 * diagnostics, replacing any that exist. Finally, the HBase table is truncated preserving its
 * partitioning and bulk loaded with new data. <br>
 * This process works on the deliberate design principle that readers of the table will have
 * implemented a retry mechanism for the brief outage during the table swap which is expected to be
 * on an e.g. weekly basis.
 */
@Builder
@Data
public class Cluster implements Serializable {
  private String hiveDB;
  private String sourceTable;
  private String hiveTablePrefix;
  private String hbaseTable;
  private int hbaseRegions;
  private String hbaseZK;
  private String targetDir;
  private int hashCountThreshold;

  private static final StructType HASH_ROW_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("gbifId", DataTypes.StringType, false),
            DataTypes.createStructField("datasetKey", DataTypes.StringType, false),
            DataTypes.createStructField("hash", DataTypes.StringType, false)
          });

  private static final StructType RELATIONSHIP_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id1", DataTypes.StringType, false),
            DataTypes.createStructField("id2", DataTypes.StringType, false),
            DataTypes.createStructField("reasons", DataTypes.StringType, false),
            DataTypes.createStructField("dataset1", DataTypes.StringType, false),
            DataTypes.createStructField("dataset2", DataTypes.StringType, false),
            DataTypes.createStructField("o1", DataTypes.StringType, false),
            DataTypes.createStructField("o2", DataTypes.StringType, false)
          });

  public static void main(String[] args) throws IOException {
    CommandLineParser.parse(args).build().run();
  }

  /** Run the full process, generating relationships and refreshing the HBase table. */
  private void run() throws IOException {
    removeTargetDir(); // fail fast if given bad config

    SparkSession spark =
        SparkSession.builder()
            .appName("Occurrence clustering")
            .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
            .enableHiveSupport()
            .getOrCreate();
    spark.sql("use " + hiveDB);

    createCandidatePairs(spark);
    Dataset<Row> relationships = generateRelationships(spark);
    generateHFiles(relationships);
    replaceHBaseTable();
  }

  /**
   * Reads the input, and through a series of record hashing creates a table of candidate record
   * pairs for proper comparison.
   */
  private void createCandidatePairs(SparkSession spark) {
    // Read the input fields needed for generating the hashes
    Dataset<Row> occurrences =
        spark.sql(
            String.format(
                "SELECT"
                    + "  gbifId, datasetKey, basisOfRecord, typeStatus, "
                    + "  CAST(taxonKey AS String) AS taxonKey, CAST(speciesKey AS String) AS speciesKey, "
                    + "  decimalLatitude, decimalLongitude, year, month, day, recordedBy, "
                    + "  recordNumber, fieldNumber, occurrenceID, otherCatalogNumbers, institutionCode, "
                    + "  collectionCode, catalogNumber "
                    + "FROM %s",
                sourceTable));
    Dataset<Row> hashes =
        occurrences
            .flatMap(
                row -> recordHashes(new RowOccurrenceFeatures(row)),
                RowEncoder.apply(HASH_ROW_SCHEMA))
            .dropDuplicates();
    spark.sql("DROP TABLE IF EXISTS " + hiveTablePrefix + "_hashes");
    hashes.write().format("parquet").saveAsTable(hiveTablePrefix + "_hashes");

    // To avoid NxN, filter to a threshold to exclude e.g. gut worm analysis datasets
    Dataset<Row> hashCounts =
        spark.sql(
            String.format(
                "SELECT hash, count(*) AS c FROM %s_hashes GROUP BY hash", hiveTablePrefix));
    spark.sql("DROP TABLE IF EXISTS " + hiveTablePrefix + "_hash_counts");
    hashCounts.write().format("parquet").saveAsTable(hiveTablePrefix + "_hash_counts");
    Dataset<Row> filteredHashes =
        spark.sql(
            String.format(
                "SELECT t1.gbifID, t1.datasetKey, t1.hash "
                    + "FROM %s_hashes t1"
                    + "  JOIN %s_hash_counts t2 ON t1.hash=t2.hash "
                    + "WHERE t2.c <= %d",
                hiveTablePrefix, hiveTablePrefix, hashCountThreshold));
    spark.sql("DROP TABLE IF EXISTS " + hiveTablePrefix + "_hashes_filtered");
    filteredHashes.write().format("parquet").saveAsTable(hiveTablePrefix + "_hashes_filtered");

    // distinct cross join to generate the candidate pairs for comparison
    Dataset<Row> candidates =
        spark.sql(
            String.format(
                "SELECT "
                    + "t1.gbifId as id1, t1.datasetKey as ds1, t2.gbifId as id2, t2.datasetKey as ds2 "
                    + "FROM %s_hashes_filtered t1 JOIN %s_hashes_filtered t2 ON t1.hash = t2.hash "
                    + "WHERE "
                    + "  t1.gbifId < t2.gbifId AND "
                    + "  t1.datasetKey != t2.datasetKey "
                    + "GROUP BY t1.gbifId, t1.datasetKey, t2.gbifId, t2.datasetKey",
                hiveTablePrefix, hiveTablePrefix));
    spark.sql("DROP TABLE IF EXISTS " + hiveTablePrefix + "_candidates");
    candidates.write().format("parquet").saveAsTable(hiveTablePrefix + "_candidates");
  }

  /** Reads the candidate pairs table, and runs the record to record comparison. */
  private Dataset<Row> generateRelationships(SparkSession spark) {
    // Spark DF naming convention requires that we alias each term to avoid naming collision while
    // still having named fields to access (i.e. not relying on the column number of the term). All
    // taxa keys are converted to String to allow shared routines between GBIF and ALA
    // (https://github.com/gbif/pipelines/issues/484)
    Dataset<Row> pairs =
        spark.sql(
            String.format(
                "SELECT "
                    + "  t1.gbifId AS t1_gbifId, t1.datasetKey AS t1_datasetKey, t1.basisOfRecord AS t1_basisOfRecord, t1.publishingorgkey AS t1_publishingOrgKey, t1.datasetName AS t1_datasetName, t1.publisher AS t1_publishingOrgName, "
                    + "  CAST(t1.kingdomKey AS String) AS t1_kingdomKey, CAST(t1.phylumKey AS String) AS t1_phylumKey, CAST(t1.classKey AS String) AS t1_classKey, CAST(t1.orderKey AS String) AS t1_orderKey, CAST(t1.familyKey AS String) AS t1_familyKey, CAST(t1.genusKey AS String) AS t1_genusKey, CAST(t1.speciesKey AS String) AS t1_speciesKey, CAST(t1.acceptedTaxonKey AS String) AS t1_acceptedTaxonKey, CAST(t1.taxonKey AS String) AS t1_taxonKey, "
                    + "  t1.scientificName AS t1_scientificName, t1.acceptedScientificName AS t1_acceptedScientificName, t1.kingdom AS t1_kingdom, t1.phylum AS t1_phylum, t1.order_ AS t1_order, t1.family AS t1_family, t1.genus AS t1_genus, t1.species AS t1_species, t1.genericName AS t1_genericName, t1.specificEpithet AS t1_specificEpithet, t1.taxonRank AS t1_taxonRank, "
                    + "  t1.typeStatus AS t1_typeStatus, t1.preparations AS t1_preparations, "
                    + "  t1.decimalLatitude AS t1_decimalLatitude, t1.decimalLongitude AS t1_decimalLongitude, t1.countryCode AS t1_countryCode, "
                    + "  t1.year AS t1_year, t1.month AS t1_month, t1.day AS t1_day, from_unixtime(t1.eventDateGte) AS t1_eventDate, "
                    + "  t1.recordNumber AS t1_recordNumber, t1.fieldNumber AS t1_fieldNumber, t1.occurrenceID AS t1_occurrenceID, t1.otherCatalogNumbers AS t1_otherCatalogNumbers, t1.institutionCode AS t1_institutionCode, t1.collectionCode AS t1_collectionCode, t1.catalogNumber AS t1_catalogNumber, "
                    + "  t1.recordedBy AS t1_recordedBy, t1.recordedByID AS t1_recordedByID, "
                    + "  t1.ext_multimedia AS t1_media, "
                    + ""
                    + "  t2.gbifId AS t2_gbifId, t2.datasetKey AS t2_datasetKey, t2.basisOfRecord AS t2_basisOfRecord, t2.publishingorgkey AS t2_publishingOrgKey, t2.datasetName AS t2_datasetName, t2.publisher AS t2_publishingOrgName, "
                    + "  CAST(t2.kingdomKey AS String) AS t2_kingdomKey, CAST(t2.phylumKey AS String) AS t2_phylumKey, CAST(t2.classKey AS String) AS t2_classKey, CAST(t2.orderKey AS String) AS t2_orderKey, CAST(t2.familyKey AS String) AS t2_familyKey, CAST(t2.genusKey AS String) AS t2_genusKey, CAST(t2.speciesKey AS String) AS t2_speciesKey, CAST(t2.acceptedTaxonKey AS String) AS t2_acceptedTaxonKey, CAST(t2.taxonKey AS String) AS t2_taxonKey, "
                    + "  t2.scientificName AS t2_scientificName, t2.acceptedScientificName AS t2_acceptedScientificName, t2.kingdom AS t2_kingdom, t2.phylum AS t2_phylum, t2.order_ AS t2_order, t2.family AS t2_family, t2.genus AS t2_genus, t2.species AS t2_species, t2.genericName AS t2_genericName, t2.specificEpithet AS t2_specificEpithet, t2.taxonRank AS t2_taxonRank, "
                    + "  t2.typeStatus AS t2_typeStatus, t2.preparations AS t2_preparations, "
                    + "  t2.decimalLatitude AS t2_decimalLatitude, t2.decimalLongitude AS t2_decimalLongitude, t2.countryCode AS t2_countryCode, "
                    + "  t2.year AS t2_year, t2.month AS t2_month, t2.day AS t2_day, from_unixtime(t2.eventDateGte) AS t2_eventDate, "
                    + "  t2.recordNumber AS t2_recordNumber, t2.fieldNumber AS t2_fieldNumber, t2.occurrenceID AS t2_occurrenceID, t2.otherCatalogNumbers AS t2_otherCatalogNumbers, t2.institutionCode AS t2_institutionCode, t2.collectionCode AS t2_collectionCode, t2.catalogNumber AS t2_catalogNumber, "
                    + "  t2.recordedBy AS t2_recordedBy, t2.recordedByID AS t2_recordedByID, "
                    + "  t2.ext_multimedia AS t2_media "
                    + ""
                    + "FROM %s h"
                    + "  JOIN %s t1 ON h.id1 = t1.gbifID "
                    + "  JOIN %s t2 ON h.id2 = t2.gbifID",
                hiveTablePrefix + "_candidates", sourceTable, sourceTable));

    // Compare all candidate pairs and generate the relationships
    Dataset<Row> relationships =
        pairs.flatMap(row -> relateRecords(row), RowEncoder.apply(RELATIONSHIP_SCHEMA));
    spark.sql("DROP TABLE IF EXISTS " + hiveTablePrefix + "_relationships");
    relationships.write().format("parquet").saveAsTable(hiveTablePrefix + "_relationships");
    return relationships;
  }

  /** Partitions the relationships to match the target table layout and creates the HFiles. */
  private void generateHFiles(Dataset<Row> relationships) throws IOException {
    // convert to HFiles, prepared with modulo salted keys
    JavaPairRDD<Tuple2<String, String>, String> sortedRelationships =
        relationships
            .javaRDD()
            .flatMapToPair(
                row -> {
                  String id1 = row.getString(0);
                  String id2 = row.getString(1);

                  // salt only on the id1 to enable prefix scanning using an occurrence ID
                  int salt = Math.abs(id1.hashCode()) % hbaseRegions;
                  String saltedRowKey = salt + ":" + id1 + ":" + id2;

                  List<Tuple2<Tuple2<String, String>, String>> cells = new ArrayList<>();
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "id1"), id1));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "id2"), id2));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "reasons"), row.getString(2)));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "dataset1"), row.getString(3)));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "dataset2"), row.getString(4)));
                  cells.add(
                      new Tuple2<>(new Tuple2<>(saltedRowKey, "occurrence1"), row.getString(5)));
                  cells.add(
                      new Tuple2<>(new Tuple2<>(saltedRowKey, "occurrence2"), row.getString(6)));

                  return cells.iterator();
                })
            .repartitionAndSortWithinPartitions(
                new SaltPrefixPartitioner(hbaseRegions), new Tuple2StringComparator());

    sortedRelationships
        .mapToPair(
            cell -> {
              ImmutableBytesWritable k = new ImmutableBytesWritable(Bytes.toBytes(cell._1._1));
              Cell row =
                  new KeyValue(
                      Bytes.toBytes(cell._1._1), // key
                      Bytes.toBytes("o"), // column family
                      Bytes.toBytes(cell._1._2), // cell
                      Bytes.toBytes(cell._2) // cell value
                      );
              return new Tuple2<>(k, row);
            })
        .saveAsNewAPIHadoopFile(
            targetDir,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf());
  }

  /** Truncates the target table preserving its layout and loads in the HFiles. */
  private void replaceHBaseTable() throws IOException {
    Configuration conf = hadoopConf();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        HTable table = new HTable(conf, hbaseTable);
        Admin admin = connection.getAdmin()) {
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

      // bulkload requires files to be in hbase ownership
      FsShell shell = new FsShell(conf);
      try {
        System.out.println("Executing chown -R hbase:hbase for " + targetDir);
        shell.run(new String[] {"-chown", "-R", "hbase:hbase", targetDir});
      } catch (Exception e) {
        throw new IOException("Unable to modify FS ownership to hbase", e);
      }

      System.out.println(String.format("Truncating table[%s]", hbaseTable));
      Instant start = Instant.now();
      admin.disableTable(table.getName());
      admin.truncateTable(table.getName(), true);
      System.out.println(
          String.format(
              "Table[%s] truncated in %d ms",
              hbaseTable, Duration.between(start, Instant.now()).toMillis()));
      System.out.println(String.format("Loading table[%s] from [%s]", hbaseTable, targetDir));
      loader.doBulkLoad(new Path(targetDir), table);
      System.out.println(
          String.format(
              "Table [%s] truncated and reloaded in %d ms",
              hbaseTable, Duration.between(start, Instant.now()).toMillis()));
      removeTargetDir();

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** Removes the target directory provided it is in the /tmp location */
  private void removeTargetDir() throws IOException {
    // defensive, cleaning only /tmp in hdfs (we assume people won't do /tmp/../...
    String regex = "/tmp/.+";
    if (targetDir.matches(regex)) {
      FsShell shell = new FsShell(new Configuration());
      try {
        System.out.println(
            String.format(
                "Deleting working directory [%s] which translates to [-rm -r -skipTrash %s ]",
                targetDir, targetDir));

        shell.run(new String[] {"-rm", "-r", "-skipTrash", targetDir});
      } catch (Exception e) {
        throw new IOException("Unable to delete the working directory", e);
      }
    } else {
      throw new IllegalArgumentIOException("Target directory must be within /tmp");
    }
  }

  /** Runs the record to record comparison */
  private Iterator<Row> relateRecords(Row row) throws IOException {
    Set<Row> relationships = new HashSet<>();

    RowOccurrenceFeatures o1 = new RowOccurrenceFeatures(row, "t1_", "t1_media");
    RowOccurrenceFeatures o2 = new RowOccurrenceFeatures(row, "t2_", "t2_media");
    RelationshipAssertion<RowOccurrenceFeatures> assertions =
        OccurrenceRelationships.generate(o1, o2);

    // store any relationship bidirectionally matching RELATIONSHIP_SCHEMA
    if (assertions != null) {
      relationships.add(
          RowFactory.create(
              assertions.getOcc1().get("gbifId"),
              assertions.getOcc2().get("gbifId"),
              assertions.getJustificationAsDelimited(),
              assertions.getOcc1().get("datasetKey"),
              assertions.getOcc2().get("datasetKey"),
              assertions.getOcc1().asJson(),
              assertions.getOcc2().asJson()));

      relationships.add(
          RowFactory.create(
              assertions.getOcc2().get("gbifId"),
              assertions.getOcc1().get("gbifId"),
              assertions.getJustificationAsDelimited(),
              assertions.getOcc2().get("datasetKey"),
              assertions.getOcc1().get("datasetKey"),
              assertions.getOcc2().asJson(),
              assertions.getOcc1().asJson()));
    }
    return relationships.iterator();
  }

  /** Creates the Hadoop configuration suitable for HDFS and HBase use. */
  private Configuration hadoopConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", hbaseZK);
    conf.set(FileOutputFormat.COMPRESS, "true");
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
    Job job = new Job(conf, "Clustering"); // name not actually used
    HTable table = new HTable(conf, hbaseTable);
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    return job.getConfiguration(); // job created a copy of the conf
  }

  /** Necessary as the Tuple2 does not implement a comparator in Java */
  static class Tuple2StringComparator implements Comparator<Tuple2<String, String>>, Serializable {
    @Override
    public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
      if (o1._1.equals(o2._1)) return o1._2.compareTo(o2._2);
      else return o1._1.compareTo(o2._1);
    }
  }
}
