package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds eMoF (ExtendedMeasurementOrFact) extension Datasets from DwC-DP assertion tables.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>event-assertion / occurrence-assertion = parent core table (surrogate FK → natural id,
 *       left outer) → eMoF rows, columns renamed to eMoF equivalents
 *   <li>assertionProtocol_fk = protocol.protocol_pk (left outer, optional)
 *   <li>material-assertion (via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}) merged
 *       into the occurrence path alongside direct occurrence-assertion
 * </ul>
 *
 * <p><b>Deferred:</b> {@code nucleotide-analysis-assertion}, {@code molecular-protocol-assertion}
 * (need careful aggregation to avoid cartesian fan-out); {@code chronometric-age-assertion}. See
 * mapping doc §4.6.
 */
@Slf4j
public class AssertionExtensionBuilder {

  static final String TABLE_EVENT_ASSERTION = "event-assertion";
  static final String TABLE_OCCURRENCE_ASSERTION = "occurrence-assertion";
  static final String TABLE_MATERIAL_ASSERTION = "material-assertion";
  static final String TABLE_PROTOCOL = "protocol";

  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType();
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

  /** Empty if event-assertion or event is absent. */
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

    if (!Arrays.asList(eventDfOpt.get().columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping event assertion extension");
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

  /** Merges occurrence-assertion with material-assertion; empty only if neither contributes. */
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

  /** Resolved via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}; same shape as {@link #buildDirectOccurrenceAssertionRows}, safe to union. */
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
   * Drops rows where {@code parentIdColumn} comes back null (dangling FK, or an entity {@link
   * MaterialJoinBuilder}'s exactly-one rule excluded) — never leaves a null-keyed row for
   * aggregation. Optionally resolves {@code assertionProtocol_fk} to {@code measurementMethod} via
   * {@code protocol}; falls back to the raw FK value if the table is absent.
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
