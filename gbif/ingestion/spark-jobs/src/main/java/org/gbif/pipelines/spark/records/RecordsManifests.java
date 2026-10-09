package org.gbif.pipelines.spark.records;

import static org.apache.spark.sql.functions.col;
import static org.gbif.pipelines.spark.records.RecordsTableWriter.DOCUMENT_DATASET_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.RecordsTableConfig;
import org.gbif.pipelines.spark.records.RecordsTableWriter.RecordType;
import scala.Tuple2;

/**
 * The row keys each dataset has in a records table, used to find the records removed from a
 * dataset. They are kept outside the dataset attempt directories, as parquet, at {@code
 * <manifestPath>/<type><state>/datasetKey=<datasetKey>}, in one of these states:
 *
 * <ul>
 *   <li>current (no suffix): the keys of the last committed load.
 *   <li>{@value #PENDING}: the keys of a load, written before its records and kept until committed.
 *   <li>{@value #PREVIOUS}: the current manifest while a commit replaces it. If the commit is
 *       interrupted, it is the last committed one.
 *   <li>{@value #STALE}: the pending manifests of failed runs, one per run. Their keys were loaded
 *       but never committed.
 * </ul>
 */
@Slf4j
class RecordsManifests {

  static final String PENDING = "_pending";
  static final String PREVIOUS = "_previous";
  static final String STALE = "_stale";
  private static final String CURRENT = "";

  private static final String ROW_KEY = "rowKey";
  private static final String PARTITION = "datasetKey";
  /**
   * Where a load writes its keys, in its working directory, before moving them to the pending root
   */
  private static final String LOAD_MANIFESTS = "records-manifests";

  private final SparkSession spark;
  private final FileSystem fileSystem;
  private final RecordsTableConfig config;
  private final RecordType type;

  RecordsManifests(
      SparkSession spark, FileSystem fileSystem, RecordsTableConfig config, RecordType type) {
    this.spark = spark;
    this.fileSystem = fileSystem;
    this.config = config;
    this.type = type;
  }

  static Path path(RecordsTableConfig config, RecordType type, String datasetKey) {
    return path(config, type, datasetKey, CURRENT);
  }

  static Path path(RecordsTableConfig config, RecordType type, String datasetKey, String state) {
    return new Path(root(config, type, state), PARTITION + "=" + datasetKey);
  }

  private static Path root(RecordsTableConfig config, RecordType type, String state) {
    String root =
        Objects.requireNonNull(config.getManifestPath(), "No records manifest path configured");
    return new Path(root + "/" + type.manifestDirectory() + state);
  }

  /**
   * Writes the keys of the documents as the pending manifests of the datasets. The pending manifest
   * of a failed run is kept as a stale one first.
   *
   * <p>Spark stages a write under the target directory and removes the staging when the job ends,
   * so loads running at the same time can't write to the shared pending root: the keys are written
   * in the working directory of the load, and each dataset is then moved to the pending root on its
   * own.
   */
  void writePending(
      List<String> datasetKeys,
      Dataset<Row> documents,
      RecordsConverter converter,
      String workingDirectory)
      throws IOException {
    for (String datasetKey : datasetKeys) {
      keepStale(datasetKey);
    }

    Path loadManifests = new Path(workingDirectory, LOAD_MANIFESTS);
    rowKeys(documents, converter)
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy(PARTITION)
        .parquet(loadManifests.toString());

    for (String datasetKey : datasetKeys) {
      Path written = new Path(loadManifests, PARTITION + "=" + datasetKey);
      // a dataset without documents has no keys
      if (fileSystem.exists(written)) {
        move(written, path(config, type, datasetKey, PENDING));
      }
    }
    fileSystem.delete(loadManifests, true);
  }

  /** Keys of the pending manifests of the datasets */
  Dataset<String> pendingKeys(List<String> datasetKeys) throws IOException {
    return readKeys(existing(datasetKeys, PENDING));
  }

  /**
   * Keys loaded by committed or failed runs of the datasets that aren't in their pending manifests
   */
  Dataset<String> removedKeys(List<String> datasetKeys) throws IOException {
    List<String> loadedBefore = currentOrPrevious(datasetKeys);
    loadedBefore.addAll(stale(datasetKeys));
    return loadedBefore.isEmpty()
        ? spark.emptyDataset(Encoders.STRING())
        : readKeys(loadedBefore).except(pendingKeys(datasetKeys));
  }

  /**
   * Replaces the manifests of the datasets with their pending ones and removes their stale ones. A
   * dataset without a pending manifest has no records left, and its manifests are removed.
   */
  void commit(List<String> datasetKeys) throws IOException {
    for (String datasetKey : datasetKeys) {
      Path current = path(config, type, datasetKey);
      Path pending = path(config, type, datasetKey, PENDING);
      Path previous = path(config, type, datasetKey, PREVIOUS);

      if (!fileSystem.exists(pending)) {
        fileSystem.delete(current, true);
        fileSystem.delete(previous, true);
      } else {
        if (fileSystem.exists(current)) {
          // a previous manifest next to the current one is a leftover of a completed commit
          fileSystem.delete(previous, true);
          move(current, previous);
        }
        // without a current manifest, the previous one of an interrupted commit is the last
        // committed one, kept until the new one is in place
        move(pending, current);
        fileSystem.delete(previous, true);
      }
      fileSystem.delete(path(config, type, datasetKey, STALE), true);
    }
  }

  /** All the keys a dataset can have in the table, committed or loaded by any run */
  Dataset<String> allKeys(String datasetKey) throws IOException {
    List<String> datasetKeys = List.of(datasetKey);
    List<String> manifests = currentOrPrevious(datasetKeys);
    manifests.addAll(stale(datasetKeys));
    manifests.addAll(existing(datasetKeys, PENDING));
    if (manifests.isEmpty()) {
      log.warn("No {} records manifest for dataset {}", type, datasetKey);
    }
    return readKeys(manifests).distinct();
  }

  /** Removes the manifests of a dataset, in every state */
  void delete(String datasetKey) throws IOException {
    for (String state : new String[] {CURRENT, PENDING, PREVIOUS, STALE}) {
      fileSystem.delete(path(config, type, datasetKey, state), true);
    }
  }

  /** Removes the manifests of every dataset, in every state */
  static void deleteAll(FileSystem fileSystem, RecordsTableConfig config, RecordType type)
      throws IOException {
    for (String state : new String[] {CURRENT, PENDING, PREVIOUS, STALE}) {
      fileSystem.delete(root(config, type, state), true);
    }
  }

  /**
   * Keeps the pending manifest of a failed run as a stale manifest, its keys were loaded but aren't
   * in any committed manifest. Several failed runs can leave one each.
   */
  private void keepStale(String datasetKey) throws IOException {
    Path pending = path(config, type, datasetKey, PENDING);
    if (!fileSystem.exists(pending)) {
      return;
    }
    Path stale =
        new Path(path(config, type, datasetKey, STALE), "run=" + System.currentTimeMillis());
    move(pending, stale);
    log.info("Kept the {} manifest of a failed run of dataset {} as {}", type, datasetKey, stale);
  }

  /** The manifests of the datasets in the given state */
  private List<String> existing(List<String> datasetKeys, String state) throws IOException {
    List<String> result = new ArrayList<>();
    for (String datasetKey : datasetKeys) {
      Path manifest = path(config, type, datasetKey, state);
      if (fileSystem.exists(manifest)) {
        result.add(manifest.toString());
      }
    }
    return result;
  }

  /** The current manifest of each dataset, or the previous one when a commit was interrupted */
  private List<String> currentOrPrevious(List<String> datasetKeys) throws IOException {
    List<String> result = new ArrayList<>();
    for (String datasetKey : datasetKeys) {
      Path current = path(config, type, datasetKey);
      Path previous = path(config, type, datasetKey, PREVIOUS);
      if (fileSystem.exists(current)) {
        result.add(current.toString());
      } else if (fileSystem.exists(previous)) {
        result.add(previous.toString());
      }
    }
    return result;
  }

  /** The stale manifests of failed runs of the datasets */
  private List<String> stale(List<String> datasetKeys) throws IOException {
    List<String> result = new ArrayList<>();
    for (String datasetKey : datasetKeys) {
      Path staleDir = path(config, type, datasetKey, STALE);
      if (fileSystem.exists(staleDir)) {
        for (FileStatus stale : fileSystem.listStatus(staleDir)) {
          if (stale.isDirectory()) {
            result.add(stale.getPath().toString());
          }
        }
      }
    }
    return result;
  }

  private Dataset<String> readKeys(List<String> manifests) {
    return manifests.isEmpty()
        ? spark.emptyDataset(Encoders.STRING())
        : spark
            .read()
            .parquet(manifests.toArray(new String[0]))
            .select(ROW_KEY)
            .as(Encoders.STRING());
  }

  private void move(Path from, Path to) throws IOException {
    // HDFS doesn't create the parent directories of a rename target
    fileSystem.mkdirs(to.getParent());
    if (!fileSystem.rename(from, to)) {
      throw new IOException("Can't move manifest " + from + " to " + to);
    }
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
        .toDF(PARTITION, ROW_KEY);
  }
}
