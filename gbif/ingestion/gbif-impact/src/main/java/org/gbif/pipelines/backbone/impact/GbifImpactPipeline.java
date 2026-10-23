package org.gbif.pipelines.backbone.impact;

import com.beust.jcommander.JCommander;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Takes the classification from verbatim data and runs it against a species lookup service
 * bypassing the key value cache. Outputs a report capturing the verbatim, current and "new" values
 * for the classifications that differ.
 *
 * <p>The report is written as Parquet to {@code <targetDir>/impact} and as a single tab delimited
 * file with a header to {@code <targetDir>/<reportFileName>}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifImpactPipeline {

  static final String SOURCE_VIEW = "gbif_impact_source";
  static final String IMPACT_DIR = "impact";
  private static final String REPORT_TMP_DIR = "_report_tmp";

  /** Verbatim columns, as named in the source table (see project readme). */
  private static final String[] VERBATIM_COLUMNS = {
    "v_kingdom",
    "v_phylum",
    "v_class",
    "v_order",
    "v_superfamily",
    "v_family",
    "v_subfamily",
    "v_tribe",
    "v_subtribe",
    "v_genus",
    "v_scientificName",
    "v_scientificNameAuthorship",
    "v_genericName",
    "v_specificEpithet",
    "v_infraSpecificEpithet",
    "v_taxonRank",
    "v_verbatimTaxonRank",
    "v_scientificNameID",
    "v_taxonID",
    "v_taxonConceptID"
  };

  /** Current interpreted columns, as named in the source table (see project readme). */
  private static final String[] CURRENT_COLUMNS = {
    "kingdom",
    "phylum",
    "class",
    "order",
    "superfamily",
    "family",
    "subfamily",
    "tribe",
    "subtribe",
    "genus",
    "subGenus",
    "species",
    "scientificName",
    "acceptedScientificName",
    "kingdomKey",
    "phylumKey",
    "classKey",
    "orderKey",
    "familyKey",
    "genusKey",
    "subGenusKey",
    "speciesKey",
    "taxonKey",
    "acceptedTaxonKey"
  };

  /** Keys considered when limiting to a scope. */
  private static final String[] SCOPE_KEY_COLUMNS = {
    "kingdomKey",
    "phylumKey",
    "classKey",
    "orderKey",
    "familyKey",
    "genusKey",
    "subGenusKey",
    "speciesKey",
    "taxonKey"
  };

  public static void main(String[] argv) throws Exception {
    GbifImpactArgs args = new GbifImpactArgs();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    boolean fromHive = args.sourcePath == null;
    if (fromHive && (args.database == null || args.table == null)) {
      throw new IllegalArgumentException(
          "Either --sourcePath or both --database and --table must be provided");
    }

    SparkSession.Builder builder = SparkSession.builder().appName(args.appName);
    if (args.master != null) {
      builder.master(args.master);
    }
    if (fromHive) {
      builder.enableHiveSupport();
      if (args.metastoreUris != null) {
        builder.config("hive.metastore.uris", args.metastoreUris);
      }
    }

    SparkSession spark = builder.getOrCreate();
    try {
      Configuration conf = spark.sparkContext().hadoopConfiguration();
      if (args.hdfsSiteConfig != null) {
        conf.addResource(new Path(args.hdfsSiteConfig));
      }
      if (args.coreSiteConfig != null) {
        conf.addResource(new Path(args.coreSiteConfig));
      }

      run(spark, args, MatchServiceFactory.rest(args.apiBaseUri));
    } finally {
      spark.stop();
    }
  }

  /** Runs the impact assessment, writing the Parquet output and the TSV report. */
  public static void run(SparkSession spark, GbifImpactArgs args, MatchServiceFactory factory)
      throws IOException {

    Path targetDir = new Path(args.targetDir);
    FileSystem fs = targetDir.getFileSystem(spark.sparkContext().hadoopConfiguration());

    // delete previous runs
    if (fs.exists(targetDir)) {
      log.info("Deleting previous run: {}", targetDir);
      fs.delete(targetDir, true);
    }

    Dataset<Row> source =
        args.sourcePath != null
            ? spark.read().parquet(args.sourcePath)
            : spark.table(args.database + "." + args.table);
    source.createOrReplaceTempView(SOURCE_VIEW);

    // only check records within scope (e.g. limit to Lepidoptera) and above the limit
    Dataset<Row> candidates = spark.sql(sourceQuery(args.minimumOccurrenceCount, args.scope));
    if (args.numPartitions > 0) {
      candidates = candidates.repartition(args.numPartitions);
    }

    candidates
        .mapPartitions(
            new ImpactMatcher(args, factory), Encoders.row(ImpactMatcher.schema(args.skipKeys)))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(args.targetDir + "/" + IMPACT_DIR);

    // read back so the lookups are not repeated when writing the report
    writeReport(spark, fs, args);
  }

  /**
   * Selects the columns used for the lookup with lower case names and string values (keys may be
   * stored as integers), filtering by the occurrence count and the optional scope.
   */
  static String sourceQuery(int minimumOccurrenceCount, Integer scope) {
    String columns =
        Arrays.stream(concat(VERBATIM_COLUMNS, CURRENT_COLUMNS))
            .map(c -> String.format("CAST(`%s` AS STRING) AS `%s`", c, c.toLowerCase()))
            .collect(Collectors.joining(",\n  "));

    String where = "occurrenceCount > " + minimumOccurrenceCount;
    if (scope != null) {
      where +=
          Arrays.stream(SCOPE_KEY_COLUMNS)
              .map(c -> String.format("CAST(`%s` AS STRING)", c))
              .collect(
                  Collectors.joining(", ", String.format(" AND '%d' IN (", scope.intValue()), ")"));
    }

    return String.format(
        "SELECT\n  %s,\n  CAST(occurrenceCount AS BIGINT) AS occurrencecount\nFROM %s\nWHERE %s",
        columns, SOURCE_VIEW, where);
  }

  /** Writes the Parquet output as a single tab delimited file with a header. */
  private static void writeReport(SparkSession spark, FileSystem fs, GbifImpactArgs args)
      throws IOException {
    Path tmpDir = new Path(args.targetDir, REPORT_TMP_DIR);
    Path report = new Path(args.targetDir, args.reportFileName);

    spark
        .read()
        .parquet(args.targetDir + "/" + IMPACT_DIR)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", "")
        .csv(tmpDir.toString());

    FileStatus[] parts = fs.globStatus(new Path(tmpDir, "part-*"));
    if (parts == null || parts.length != 1) {
      throw new IOException("Expected a single report part file in " + tmpDir);
    }
    fs.rename(parts[0].getPath(), report);
    fs.delete(tmpDir, true);
    log.info("Written report to: {}", report);
  }

  private static String[] concat(String[] a, String[] b) {
    String[] result = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
}
