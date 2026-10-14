package org.gbif.pipelines.spark.records;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;

/**
 * Writes the API representation of records to HBase and removes the records that are no longer part
 * of their dataset.
 *
 * <p>A load first writes the keys of the documents as pending manifests ({@link RecordsManifests}),
 * then the records ({@link RecordsTable}). The records loaded before that aren't in the new load
 * are deleted, and the pending manifests replace the current ones, only once {@link
 * RecordsLoad#commit()} is called, after the index no longer returns the removed keys. A failed run
 * therefore leaves the removed records and the manifests in place, and the next run repeats the
 * deletes, of the keys the failed run loaded too.
 *
 * <p>A single dataset (incremental indexing) and many datasets (full builds) are loaded the same
 * way.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RecordsTableWriter {

  /** Column holding the dataset key in the indexed documents */
  public static final String DOCUMENT_DATASET_KEY = "datasetKey";

  /** Kinds of records, each in its own table */
  public enum RecordType {
    OCCURRENCE,
    EVENT;

    RecordsConverter converter() {
      return this == OCCURRENCE ? RecordsConverter.forOccurrences() : RecordsConverter.forEvents();
    }

    public String table(RecordsTableConfig config) {
      String table = this == OCCURRENCE ? config.getOccurrenceTable() : config.getEventTable();
      return Objects.requireNonNull(table, "No HBase records table configured for " + this);
    }

    String manifestDirectory() {
      return name().toLowerCase();
    }
  }

  /**
   * Result of a load, the removed records are deleted and the manifests of the loaded datasets are
   * replaced on commit
   */
  public static class RecordsLoad {
    @Getter private final long loaded;
    /** Records removed from the datasets, known once committed */
    @Getter private long removed;

    private final RecordsManifests manifests;
    private final RecordsTable table;
    private final List<String> datasetKeys;

    private RecordsLoad(
        long loaded, RecordsManifests manifests, RecordsTable table, List<String> datasetKeys) {
      this.loaded = loaded;
      this.manifests = manifests;
      this.table = table;
      this.datasetKeys = datasetKeys;
    }

    /**
     * Deletes the records no longer in the loaded datasets and replaces their manifests with the
     * keys of this load. Call it once the index no longer returns the removed keys, as it can't be
     * undone.
     *
     * @return the number of records removed
     */
    public long commit() throws IOException {
      removed = table.delete(manifests.removedKeys(datasetKeys));
      log.info(
          "Removed {} records of {} datasets from {}", removed, datasetKeys.size(), table.name());

      // the manifests are replaced last, if the deletes fail the next run repeats them
      manifests.commit(datasetKeys);
      return removed;
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
   * Loads the records of the given datasets. The ones loaded by previous runs that are no longer
   * present are deleted by {@link RecordsLoad#commit()}, to call once the index no longer returns
   * them.
   *
   * @param datasetAttempts the datasets to load and the attempt each one comes from. A dataset
   *     without documents has all its records removed.
   * @param documents the documents indexed in Elasticsearch, with a {@value DOCUMENT_DATASET_KEY}
   *     column
   * @param workingDirectory directory where the keys and HFiles are staged
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
    RecordsManifests manifests = new RecordsManifests(spark, fileSystem, tableConfig, type);
    RecordsTable table = new RecordsTable(hbaseConf, tableConfig, type);
    List<String> datasetKeys =
        datasetAttempts.keySet().stream().sorted().collect(Collectors.toList());

    Dataset<Row> datasetDocuments =
        documents.where(col(DOCUMENT_DATASET_KEY).isin(datasetKeys.toArray()));

    // the new keys are written first as pending manifests, and counted from there
    manifests.writePending(datasetKeys, datasetDocuments, type.converter(), workingDirectory);
    long loaded = manifests.pendingKeys(datasetKeys).count();

    if (loaded > 0) {
      table.write(spark, fileSystem, datasetDocuments, datasetAttempts, loaded, workingDirectory);
    }

    log.info(
        "Loaded {} {} records of {} datasets into {}",
        loaded,
        type,
        datasetKeys.size(),
        table.name());
    return new RecordsLoad(loaded, manifests, table, datasetKeys);
  }

  /** Deletes all records of a dataset and its manifests */
  public static long deleteDataset(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      Configuration hbaseConf,
      RecordType type,
      String datasetKey)
      throws IOException {

    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    RecordsManifests manifests = new RecordsManifests(spark, fileSystem, tableConfig, type);

    long deleted =
        new RecordsTable(hbaseConf, tableConfig, type).delete(manifests.allKeys(datasetKey));
    manifests.delete(datasetKey);

    log.info("Deleted {} {} records of dataset {}", deleted, type, datasetKey);
    return deleted;
  }

  /**
   * Empties the table of a record type, keeping its regions, and removes all its manifests, to
   * build the table from scratch. The table is emptied first: if removing the manifests fails, they
   * only list keys that are no longer in the table.
   */
  public static void truncate(
      FileSystem fileSystem, PipelinesConfig config, Configuration hbaseConf, RecordType type)
      throws IOException {

    RecordsTableConfig tableConfig = config.getRecordsTableConfig();
    String table = type.table(tableConfig);
    if (sharedTables(config).contains(table)) {
      throw new IllegalArgumentException(
          "Records table " + table + " is also the keygen or fragments table, not truncating it");
    }

    new RecordsTable(hbaseConf, tableConfig, type).truncate();
    RecordsManifests.deleteAll(fileSystem, tableConfig, type);
    log.info("Truncated {} and removed its {} manifests", table, type);
  }

  /** Tables of other components, never to be used as records tables */
  private static List<String> sharedTables(PipelinesConfig config) {
    List<String> tables = new ArrayList<>();
    tables.add(config.getFragmentsTable());
    if (config.getKeygen() != null) {
      tables.add(config.getKeygen().getOccurrenceTable());
      tables.add(config.getKeygen().getLookupTable());
      tables.add(config.getKeygen().getCounterTable());
    }
    return tables;
  }
}
