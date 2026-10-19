package org.gbif.pipelines.spark.diag;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.gbif.pipelines.spark.Directories.IDENTIFIERS;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_JSON;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_VERBATIM;
import static org.gbif.pipelines.spark.Directories.SIMPLE_OCCURRENCE;
import static org.gbif.pipelines.spark.util.PipelinesConfigUtil.loadConfig;
import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.gbif.pipelines.spark.util.PipelineArgs;

/**
 * Finds identifiers (uniqueKeys) that were assigned during interpretation but are missing from the
 * interpreted output of the same attempt, e.g. records dropped because they share a gbifID with
 * another record.
 *
 * <p>For each dataset the latest attempt with {@code json/_SUCCESS} is used:
 *
 * <ul>
 *   <li>{@code loadedIdentifiers} - IdentifierRecords embedded in {@code simple_occurrence}
 *       (Occurrence.identifier)
 *   <li>{@code candidateIdentifiers} - IdentifierRecords in {@code identifiers}
 * </ul>
 *
 * The two are full outer joined on (datasetKey, uniqueKey) and only the keys without a match on the
 * other side are kept. Records without a uniqueKey (triplet only) can't be joined and are excluded.
 *
 * <p>The triplet ({@code ic|cc|cn|null}) of each orphaned record is added too, as a remaining
 * triplet lookup row re-links a deleted occurrenceID to its old gbifID. It is the record's
 * associatedKey, or if that wasn't set it is built from the record in {@code verbatim} (the
 * orphaned records are not in simple_occurrence, and BasicRecord has no triplet fields).
 *
 * <p>The output is a single CSV file with the columns datasetKey, uniqueKey (occurrenceID or
 * triplet), hbaseKey (the salted lookup table row key, {@code NN:datasetKey|uniqueKey}) and keyType
 * (OCCURRENCE_ID or TRIPLET).
 */
@Slf4j
public class OrphanedIdentifiersPipeline {

  public static final String DEFAULT_OUTPUT_FILE = "/tmp/orphaned_keys.csv";

  static final String LOADED_VIEW = "loadedIdentifiers";
  static final String CANDIDATE_VIEW = "candidateIdentifiers";
  static final String ORPHANED_VIEW = "orphanedKeys";
  static final String ORPHANED_TRIPLET_VIEW = "orphanedTriplets";
  static final String VERBATIM_TRIPLET_VIEW = "verbatimTriplets";
  static final String HBASE_KEY_UDF = "hbaseKey";
  static final String TRIPLET_UDF = "tripletKey";

  static final String OCCURRENCE_ID = "OCCURRENCE_ID";
  static final String TRIPLET = "TRIPLET";

  private static final String SUCCESS_MARKER = "_SUCCESS";

  // dataset key from .../<datasetKey>/<attempt>/simple_occurrence/part-...
  private static final String DATASET_KEY_FROM_PATH =
      "/([0-9a-fA-F-]{36})/[0-9]+/" + SIMPLE_OCCURRENCE + "/";

  private static final StructType IDENTIFIER_SCHEMA =
      new StructType()
          .add("id", DataTypes.StringType)
          .add("internalId", DataTypes.StringType)
          .add("uniqueKey", DataTypes.StringType)
          .add("associatedKey", DataTypes.StringType);

  private static final StructType VERBATIM_TRIPLET_SCHEMA =
      new StructType()
          .add("datasetKey", DataTypes.StringType)
          .add("id", DataTypes.StringType)
          .add("triplet", DataTypes.StringType);

  @Parameters(separators = "=")
  private static class Args extends PipelineArgs {

    @Parameter(names = "--datasetIds", description = "Comma separated dataset keys")
    private List<String> datasetIds = new ArrayList<>();

    @Parameter(
        names = "--datasetIdsFile",
        description =
            "File with one dataset key per line (blank lines and # comments ignored), "
                + "default filesystem unless a scheme like file:/// is given")
    private String datasetIdsFile;

    @Parameter(
        names = "--ingestPath",
        description = "Root of the dataset/attempt directories, defaults to outputPath in config")
    private String ingestPath;

    @Parameter(names = "--outputFile", description = "CSV file to write, on the default filesystem")
    private String outputFile = DEFAULT_OUTPUT_FILE;
  }

  public static void main(String[] argsv) throws Exception {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);
    SparkSession spark = null;
    FileSystem fileSystem = null;

    try {
      /* ############ standard init block ########## */
      spark = getSparkSession(args.master, args.appName, config, (builder, cfg) -> {});
      fileSystem = getFileSystem(spark, config);
      /* ############ standard init block - end ########## */

      Set<String> datasetKeys =
          IdentifierExportPipeline.readDatasetKeys(spark, args.datasetIds, args.datasetIdsFile);
      String ingestPath = args.ingestPath != null ? args.ingestPath : config.getOutputPath();

      run(spark, fileSystem, datasetKeys, ingestPath, args.outputFile);
    } finally {
      if (fileSystem != null) {
        fileSystem.close();
      }
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }
    System.exit(0);
  }

  /**
   * Writes the orphaned keys of the given datasets and their triplets to {@code outputFile}.
   *
   * @return the number of rows written
   */
  public static long run(
      SparkSession spark,
      FileSystem fs,
      Collection<String> datasetKeys,
      String ingestPath,
      String outputFile)
      throws IOException {

    long start = System.currentTimeMillis();

    // 1. latest successful attempt per dataset
    Map<String, Path> attempts = new LinkedHashMap<>();
    for (String datasetKey : datasetKeys) {
      Optional<Path> attempt = findLastSuccessfulAttempt(fs, new Path(ingestPath, datasetKey));
      if (attempt.isEmpty()) {
        log.warn("Skipping {}, no attempt with {}/{}", datasetKey, OCCURRENCE_JSON, SUCCESS_MARKER);
      } else if (!fs.exists(new Path(attempt.get(), SIMPLE_OCCURRENCE))
          || !fs.exists(new Path(attempt.get(), IDENTIFIERS))) {
        log.warn(
            "Skipping {}, {} or {} missing in {}",
            datasetKey,
            SIMPLE_OCCURRENCE,
            IDENTIFIERS,
            attempt.get());
      } else {
        log.info("Using {} for {}", attempt.get(), datasetKey);
        attempts.put(datasetKey, attempt.get());
      }
    }
    log.info(
        "Found successful attempts for {} of {} datasets", attempts.size(), datasetKeys.size());

    if (attempts.isEmpty()) {
      throw new IllegalStateException("None of the datasets has a successful attempt");
    }

    // 2. loadedIdentifiers - from Occurrence.identifier in simple_occurrence
    String[] simplePaths =
        attempts.values().stream()
            .map(p -> new Path(p, SIMPLE_OCCURRENCE).toString())
            .toArray(String[]::new);

    Dataset<Row> loaded =
        spark
            .read()
            .parquet(simplePaths)
            .select(
                regexp_extract(input_file_name(), DATASET_KEY_FROM_PATH, 1).as("datasetKey"),
                from_json(col("identifier"), IDENTIFIER_SCHEMA).as("ir"))
            .select(
                col("datasetKey"),
                col("ir.id").as("id"),
                col("ir.internalId").as("internalId"),
                col("ir.uniqueKey").as("uniqueKey"),
                col("ir.associatedKey").as("associatedKey"));
    loaded.createOrReplaceTempView(LOADED_VIEW);

    // 3. candidateIdentifiers - from identifiers, one read per dataset to add its key
    Dataset<Row> candidates = null;
    for (Map.Entry<String, Path> e : attempts.entrySet()) {
      Dataset<Row> ds =
          spark
              .read()
              .parquet(new Path(e.getValue(), IDENTIFIERS).toString())
              .select(
                  lit(e.getKey()).as("datasetKey"),
                  col("id"),
                  col("internalId"),
                  col("uniqueKey"),
                  col("associatedKey"));
      candidates = candidates == null ? ds : candidates.unionByName(ds);
    }
    candidates.createOrReplaceTempView(CANDIDATE_VIEW);

    // 4. verbatimTriplets - triplets built from the verbatim records, which unlike
    // simple_occurrence
    // still contain the dropped records
    registerUdfs(spark);
    loadVerbatimTriplets(spark, fs, attempts).createOrReplaceTempView(VERBATIM_TRIPLET_VIEW);

    // 5. full outer join on the unique key, keep only the keys without a match
    spark
        .sql(
            "SELECT COALESCE(c.datasetKey, l.datasetKey) AS datasetKey, "
                + "       COALESCE(c.uniqueKey, l.uniqueKey) AS uniqueKey "
                + "FROM (SELECT DISTINCT datasetKey, uniqueKey FROM "
                + CANDIDATE_VIEW
                + "      WHERE uniqueKey IS NOT NULL AND uniqueKey <> '') c "
                + "FULL OUTER JOIN (SELECT DISTINCT datasetKey, uniqueKey FROM "
                + LOADED_VIEW
                + "      WHERE uniqueKey IS NOT NULL AND uniqueKey <> '') l "
                + "  ON c.datasetKey = l.datasetKey AND c.uniqueKey = l.uniqueKey "
                + "WHERE c.uniqueKey IS NULL OR l.uniqueKey IS NULL")
        .createOrReplaceTempView(ORPHANED_VIEW);

    // 6. triplets of the orphaned records: the associatedKey keygen used, or if it wasn't set (run
    // with tripletValid=false) the triplet from the verbatim record, as older crawls may have
    // written it
    spark
        .sql(
            "SELECT DISTINCT o.datasetKey, COALESCE(NULLIF(r.associatedKey, ''), v.triplet) AS uniqueKey "
                + "FROM "
                + ORPHANED_VIEW
                + " o "
                + "JOIN (SELECT datasetKey, id, uniqueKey, associatedKey FROM "
                + CANDIDATE_VIEW
                + "      UNION ALL "
                + "      SELECT datasetKey, id, uniqueKey, associatedKey FROM "
                + LOADED_VIEW
                + ") r ON r.datasetKey = o.datasetKey AND r.uniqueKey = o.uniqueKey "
                + "LEFT JOIN "
                + VERBATIM_TRIPLET_VIEW
                + " v ON v.datasetKey = r.datasetKey AND v.id = r.id "
                + "WHERE COALESCE(NULLIF(r.associatedKey, ''), v.triplet) IS NOT NULL")
        .createOrReplaceTempView(ORPHANED_TRIPLET_VIEW);

    Dataset<Row> output =
        spark
            .sql(
                "SELECT datasetKey, uniqueKey, "
                    + HBASE_KEY_UDF
                    + "(datasetKey, uniqueKey) AS hbaseKey, '"
                    + OCCURRENCE_ID
                    + "' AS keyType FROM "
                    + ORPHANED_VIEW
                    + " UNION ALL "
                    + "SELECT datasetKey, uniqueKey, "
                    + HBASE_KEY_UDF
                    + "(datasetKey, uniqueKey) AS hbaseKey, '"
                    + TRIPLET
                    + "' AS keyType FROM "
                    + ORPHANED_TRIPLET_VIEW)
            .orderBy("datasetKey", "keyType", "uniqueKey");

    // 7. single csv file
    long count = writeSingleCsv(fs, output, outputFile);

    log.info(
        "Wrote {} orphaned keys and triplets from {} datasets to {} in {} secs",
        count,
        attempts.size(),
        outputFile,
        (System.currentTimeMillis() - start) / 1000);
    return count;
  }

  /**
   * Record id and triplet of every verbatim record of the attempts, from {@code verbatim}. Attempts
   * without that directory are skipped, their triplets then only come from associatedKey.
   */
  private static Dataset<Row> loadVerbatimTriplets(
      SparkSession spark, FileSystem fs, Map<String, Path> attempts) throws IOException {
    Dataset<Row> triplets = spark.createDataFrame(Collections.emptyList(), VERBATIM_TRIPLET_SCHEMA);
    for (Map.Entry<String, Path> e : attempts.entrySet()) {
      Path verbatim = new Path(e.getValue(), OCCURRENCE_VERBATIM);
      if (!fs.exists(verbatim)) {
        log.warn("No {} for {}, triplets only from associatedKey", verbatim, e.getKey());
        continue;
      }
      Dataset<Row> ds =
          spark
              .read()
              .parquet(verbatim.toString())
              .select(
                  lit(e.getKey()).as("datasetKey"),
                  col("id"),
                  callUDF(
                          TRIPLET_UDF,
                          col("coreTerms").getItem(DwcTerm.institutionCode.qualifiedName()),
                          col("coreTerms").getItem(DwcTerm.collectionCode.qualifiedName()),
                          col("coreTerms").getItem(DwcTerm.catalogNumber.qualifiedName()))
                      .as("triplet"))
              .filter(col("triplet").isNotNull());
      triplets = triplets.unionByName(ds);
    }
    return triplets;
  }

  static void registerUdfs(SparkSession spark) {
    registerHbaseKeyUdf(spark);
    spark
        .udf()
        .register(
            TRIPLET_UDF,
            (UDF3<String, String, String, String>) OrphanedIdentifiersPipeline::triplet,
            DataTypes.StringType);
  }

  /** Triplet as built by GbifIdInterpreter: trimmed values, ic|cc|cn|null, null if any is empty. */
  static String triplet(String institutionCode, String collectionCode, String catalogNumber) {
    return OccurrenceKeyBuilder.buildKey(
            trim(institutionCode), trim(collectionCode), trim(catalogNumber))
        .orElse(null);
  }

  private static String trim(String value) {
    return value == null ? null : value.trim();
  }

  /** Registers a UDF returning the salted lookup table row key: NN:datasetKey|uniqueKey */
  static void registerHbaseKeyUdf(SparkSession spark) {
    spark
        .udf()
        .register(
            HBASE_KEY_UDF,
            (UDF2<String, String, String>) OrphanedIdentifiersPipeline::hbaseKey,
            DataTypes.StringType);
  }

  /** Salted lookup table row key, as written by HBaseLockingKeyService. */
  static String hbaseKey(String datasetKey, String uniqueKey) {
    Set<String> keys = OccurrenceKeyBuilder.buildKeys(Collections.singleton(uniqueKey), datasetKey);
    return Bytes.toString(
        HBaseStore.saltKey(keys.iterator().next(), HBaseLockingKeyService.NUMBER_OF_BUCKETS));
  }

  /** Latest numeric attempt directory that contains json/_SUCCESS. */
  static Optional<Path> findLastSuccessfulAttempt(FileSystem fs, Path datasetDir)
      throws IOException {
    if (!fs.exists(datasetDir)) {
      return Optional.empty();
    }
    List<FileStatus> attempts =
        Arrays.stream(fs.listStatus(datasetDir))
            .filter(FileStatus::isDirectory)
            .filter(s -> s.getPath().getName().matches("\\d+"))
            .sorted(
                Comparator.comparingLong((FileStatus s) -> Long.parseLong(s.getPath().getName()))
                    .reversed())
            .toList();
    for (FileStatus attempt : attempts) {
      if (fs.exists(new Path(new Path(attempt.getPath(), OCCURRENCE_JSON), SUCCESS_MARKER))) {
        return Optional.of(attempt.getPath());
      }
    }
    return Optional.empty();
  }

  private static long writeSingleCsv(FileSystem fs, Dataset<Row> rows, String outputFile)
      throws IOException {
    Path target = new Path(outputFile);
    Path tmpDir = new Path(target.getParent(), "." + target.getName() + "_tmp");

    rows.coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(tmpDir.toString());

    FileStatus[] parts = fs.globStatus(new Path(tmpDir, "part-*.csv"));
    if (parts == null || parts.length != 1) {
      throw new IllegalStateException("Expected one csv part file in " + tmpDir);
    }
    fs.delete(target, false);
    if (!fs.rename(parts[0].getPath(), target)) {
      throw new IOException("Could not move " + parts[0].getPath() + " to " + target);
    }
    fs.delete(tmpDir, true);

    return rows.sparkSession().read().option("header", "true").csv(target.toString()).count();
  }
}
