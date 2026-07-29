package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds eMoF (ExtendedMeasurementOrFact) extension Datasets from DwC-DP assertion tables.
 *
 * <p>DwC-DP represents measurements and facts as assertion rows linked to their parent entity via
 * an internal surrogate FK. This builder resolves those FKs to natural DwC identifiers ({@code
 * eventID} / {@code occurrenceID}), optionally resolves {@code assertionProtocol_fk} to a
 * human-readable description via the {@code protocol} table, and renames all DwC-DP assertion
 * column names to their DwC-A eMoF equivalents before aggregating into a JSON column.
 *
 * <p>{@code buildOccurrenceAssertionExtension} additionally merges in {@code material-assertion} —
 * measurements/facts about the specimen that's exactly-one evidence for the occurrence (resolved
 * via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}) — into the same eMoF extension as
 * the occurrence's own direct {@code occurrence-assertion} rows, same reasoning as {@link
 * MediaExtensionBuilder}'s equivalent merge: once {@link MaterialJoinBuilder} has established a 1:1
 * occurrence/material relationship, a specimen measurement and an occurrence measurement both just
 * describe the same real-world thing. Both paths reuse the same generic {@link
 * #resolveAssertionLinks}/{@link #remapAssertionColumns} — only the FK column and parent table
 * differ — so both produce an identical eMoF-renamed column shape, safe to union before
 * aggregating.
 *
 * <p>Both {@code build*Extension} methods return {@link Optional#empty()} when nothing on either
 * side contributes any rows.
 */
@Slf4j
public class AssertionExtensionBuilder {

  static final String TABLE_EVENT_ASSERTION = "event-assertion";
  static final String TABLE_OCCURRENCE_ASSERTION = "occurrence-assertion";
  static final String TABLE_MATERIAL_ASSERTION = "material-assertion";
  static final String TABLE_PROTOCOL = "protocol";

  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact";
  public static final String COL_ASSERTION_EXT_JSON = "assertionExtJson";

  // DwC-DP assertion column names → DwC-A eMoF term names.
  // The FK columns (event_fk / occurrence_fk / materialEntity_fk) are handled separately in
  // resolveAssertionLinks.
  // assertionProtocol_fk → measurementMethod is lossy: the FK value (an ID string) is stored as
  // free text because a protocol lookup table is not always available at this stage.
  private static final Map<String, String> ASSERTION_TO_EMOF_COLUMNS =
      Map.ofEntries(
          Map.entry("assertionID", "measurementID"),
          Map.entry("assertionType", "measurementType"),
          Map.entry("assertionTypeIRI", "measurementTypeID"),
          Map.entry("assertionValue", "measurementValue"),
          Map.entry("assertionValueIRI", "measurementValueID"),
          Map.entry("assertionUnit", "measurementUnit"),
          Map.entry("assertionUnitIRI", "measurementUnitID"),
          Map.entry("assertionError", "measurementAccuracy"),
          Map.entry("assertionBy", "measurementDeterminedBy"),
          Map.entry("assertionMadeDate", "measurementDeterminedDate"),
          Map.entry("assertionRemarks", "measurementRemarks"),
          Map.entry("assertionProtocol_fk", "measurementMethod"));

  private AssertionExtensionBuilder() {}

  /**
   * Returns a two-column Dataset {@code (eventID, assertionExtJson)} built from the {@code
   * event-assertion} table. {@code event_fk} is resolved to the natural {@code eventID} via the
   * {@code event} table; {@code assertionProtocol_fk} is resolved to {@code measurementMethod} via
   * the {@code protocol} table when available. Empty if either the {@code event-assertion} or
   * {@code event} table is absent.
   */
  public static Optional<Dataset<Row>> buildEventAssertionExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> assertionDfOpt = loader.load(TABLE_EVENT_ASSERTION);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");

    if (assertionDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      log.debug(
          "Skipping event assertion extension: event-assertion present={}, event present={}",
          assertionDfOpt.isPresent(),
          eventDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> df =
        remapAssertionColumns(
            resolveAssertionLinks(
                loader, assertionDfOpt.get(), "event_fk", eventDfOpt.get(), "event_pk", "eventID"));

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, df, df.columns(), "eventID", COL_ASSERTION_EXT_JSON));
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, assertionExtJson)}, merging {@code
   * occurrence-assertion} rows with {@code material-assertion} rows from the occurrence's own
   * material. {@code assertionProtocol_fk} is resolved to {@code measurementMethod} via the {@code
   * protocol} table when available, on both sides independently. Empty only if neither source
   * contributes anything.
   */
  public static Optional<Dataset<Row>> buildOccurrenceAssertionExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> fromOccurrenceAssertion = buildDirectOccurrenceAssertionRows(loader);
    Optional<Dataset<Row>> fromMaterialAssertion = buildMaterialAssertionRows(loader);

    Optional<Dataset<Row>> combined =
        unionIfBothPresent(fromOccurrenceAssertion, fromMaterialAssertion);
    if (combined.isEmpty()) {
      log.debug(
          "Skipping occurrence assertion extension: no direct or material-linked assertions found");
      return Optional.empty();
    }

    Dataset<Row> df = combined.get();
    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, df, df.columns(), "occurrenceID", COL_ASSERTION_EXT_JSON));
  }

  /**
   * Row-level (pre-aggregation, already eMoF-remapped) rows from the direct {@code
   * occurrence-assertion} link.
   */
  private static Optional<Dataset<Row>> buildDirectOccurrenceAssertionRows(TableLoader loader) {
    Optional<Dataset<Row>> assertionDfOpt = loader.load(TABLE_OCCURRENCE_ASSERTION);
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");

    if (assertionDfOpt.isEmpty() || occurrenceDfOpt.isEmpty()) {
      log.debug(
          "Skipping direct occurrence-assertion rows: occurrence-assertion present={}, occurrence present={}",
          assertionDfOpt.isPresent(),
          occurrenceDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> df =
        remapAssertionColumns(
            resolveAssertionLinks(
                loader,
                assertionDfOpt.get(),
                "occurrence_fk",
                occurrenceDfOpt.get(),
                "occurrence_pk",
                "occurrenceID"));
    return Optional.of(df);
  }

  /**
   * Row-level (pre-aggregation, already eMoF-remapped) rows from {@code material-assertion},
   * resolved through {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks} down to the
   * occurrence the material record is exactly-one evidence for. Reuses the same {@link
   * #resolveAssertionLinks}/{@link #remapAssertionColumns} as the direct path, just with {@code
   * materialEntity_fk}/{@code materialEntity_pk} instead of {@code occurrence_fk}/{@code
   * occurrence_pk} — identical output column shape, safe to union with {@link
   * #buildDirectOccurrenceAssertionRows}.
   */
  private static Optional<Dataset<Row>> buildMaterialAssertionRows(TableLoader loader) {
    Optional<Dataset<Row>> materialAssertionDfOpt = loader.load(TABLE_MATERIAL_ASSERTION);
    if (materialAssertionDfOpt.isEmpty()) {
      log.debug("No material-assertion table present; skipping material-assertion merge");
      return Optional.empty();
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug(
          "No single-material-per-occurrence links available; skipping material-assertion merge");
      return Optional.empty();
    }

    Dataset<Row> df =
        remapAssertionColumns(
            resolveAssertionLinks(
                loader,
                materialAssertionDfOpt.get(),
                "materialEntity_fk",
                materialLinksOpt.get(),
                "materialEntity_pk",
                "occurrenceID"));
    return Optional.of(df);
  }

  /**
   * Unions two optional row-sets when both are present, returns whichever one is present otherwise,
   * or {@link Optional#empty()} if neither is.
   */
  private static Optional<Dataset<Row>> unionIfBothPresent(
      Optional<Dataset<Row>> a, Optional<Dataset<Row>> b) {
    if (a.isPresent() && b.isPresent()) {
      return Optional.of(a.get().unionByName(b.get()));
    }
    return a.isPresent() ? a : b;
  }

  /**
   * Joins an assertion Dataset against its parent entity table to replace the internal FK with the
   * natural DwC identifier. Rows where {@code parentIdColumn} comes back null (the FK didn't match
   * any row in {@code parentDf} at all — a genuinely dangling reference, or, for the
   * material-assertion merge, an entity {@link MaterialJoinBuilder}'s exactly-one rule deliberately
   * excluded) are dropped rather than allowed through: a left-outer join by itself would let such a
   * row survive with a null id, which {@link ExtensionAggregator#aggregateAsJsonByKey} would then
   * group under a null key — a real but meaningless output row, not nothing.
   *
   * <p>Optionally joins the {@code protocol} table — if present — to resolve {@code
   * assertionProtocol_fk} into a human-readable {@code measurementMethod} string; when the protocol
   * table is absent the raw FK value is kept under {@code assertionProtocol_fk} and {@link
   * #remapAssertionColumns} will rename it to {@code measurementMethod} as a fallback.
   */
  private static Dataset<Row> resolveAssertionLinks(
      TableLoader loader,
      Dataset<Row> assertionDf,
      String fkColumn,
      Dataset<Row> parentDf,
      String parentPkColumn,
      String parentIdColumn) {

    Dataset<Row> result =
        assertionDf
            .join(
                parentDf.select(parentPkColumn, parentIdColumn),
                assertionDf.col(fkColumn).equalTo(parentDf.col(parentPkColumn)),
                "left_outer")
            .drop(parentDf.col(parentPkColumn))
            .drop(assertionDf.col(fkColumn))
            .filter(functions.col(parentIdColumn).isNotNull());

    Optional<Dataset<Row>> protocolDfOpt = loader.load(TABLE_PROTOCOL);
    if (protocolDfOpt.isPresent()
        && Arrays.asList(result.columns()).contains("assertionProtocol_fk")) {
      Dataset<Row> protocolDf = protocolDfOpt.get().select("protocol_pk", "protocolDescription");
      result =
          result
              .join(
                  protocolDf,
                  result.col("assertionProtocol_fk").equalTo(protocolDf.col("protocol_pk")),
                  "left_outer")
              .drop(protocolDf.col("protocol_pk"))
              .drop("assertionProtocol_fk")
              .withColumnRenamed("protocolDescription", "measurementMethod");
    }

    return result;
  }

  /** Renames DwC-DP assertion column names to their DwC-A eMoF equivalents. */
  private static Dataset<Row> remapAssertionColumns(Dataset<Row> df) {
    Dataset<Row> result = df;
    for (Map.Entry<String, String> e : ASSERTION_TO_EMOF_COLUMNS.entrySet()) {
      if (Arrays.asList(result.columns()).contains(e.getKey())) {
        result = result.withColumnRenamed(e.getKey(), e.getValue());
      }
    }
    return result;
  }
}
