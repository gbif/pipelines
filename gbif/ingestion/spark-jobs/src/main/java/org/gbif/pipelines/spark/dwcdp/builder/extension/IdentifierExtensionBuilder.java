package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds {@code Extension.IDENTIFIER} Datasets for the occurrence-core and event-core paths.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>occurrence-identifier (direct) + material-identifier (via {@link
 *       MaterialJoinBuilder#singleMaterialOccurrenceLinks}) → occurrence path, merged
 *   <li>event-identifier (direct) → event path
 * </ul>
 *
 * <p>Row type is confirmed to exist; no confirmed downstream interpreter reads it (unlike
 * Multimedia/eMoF/Humboldt/Identification). See mapping doc §4.7.
 */
@Slf4j
public class IdentifierExtensionBuilder {

  public static final String TABLE_EVENT_IDENTIFIER = "event-identifier";
  static final String TABLE_OCCURRENCE_IDENTIFIER = "occurrence-identifier";
  static final String TABLE_MATERIAL_IDENTIFIER = "material-identifier";

  /** Extension.IDENTIFIER.getRowType(). */
  public static final String ROW_TYPE_IDENTIFIER = Extension.IDENTIFIER.getRowType();

  public static final String COL_IDENTIFIER_EXT_JSON = "identifierExtJson";

  private IdentifierExtensionBuilder() {}

  /**
   * Returns a two-column Dataset {@code (occurrenceID, identifierExtJson)}, merging {@code
   * occurrence-identifier} rows with {@code material-identifier} rows from the occurrence's own
   * material. Returns {@link Optional#empty()} only if neither source contributes anything.
   */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> fromOccurrenceIdentifier = buildDirectOccurrenceIdentifierRows(loader);
    Optional<Dataset<Row>> fromMaterialIdentifier = buildMaterialIdentifierRows(loader);

    Optional<Dataset<Row>> combined =
        unionIfBothPresent(fromOccurrenceIdentifier, fromMaterialIdentifier);
    if (combined.isEmpty()) {
      log.debug("Skipping identifier extension: no direct or material-linked identifiers found");
      return Optional.empty();
    }

    Dataset<Row> df = combined.get();
    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, df, df.columns(), "occurrenceID", COL_IDENTIFIER_EXT_JSON));
  }

  /**
   * Returns a two-column Dataset {@code (eventID, identifierExtJson)} from direct {@code
   * event-identifier} rows, or {@link Optional#empty()} when either required table is absent or no
   * identifier row resolves to an event.
   */
  public static Optional<Dataset<Row>> buildEvent(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> identifierDfOpt = loader.load(TABLE_EVENT_IDENTIFIER);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");

    if (identifierDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      log.debug(
          "Skipping event identifier extension: event-identifier present={}, event present={}",
          identifierDfOpt.isPresent(),
          eventDfOpt.isPresent());
      return Optional.empty();
    }

    if (!Arrays.asList(eventDfOpt.get().columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping event identifier extension");
      return Optional.empty();
    }

    Dataset<Row> resolved =
        resolveToParentId(
            identifierDfOpt.get(), "event_fk", eventDfOpt.get(), "event_pk", "eventID");
    if (resolved.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, resolved, resolved.columns(), "eventID", COL_IDENTIFIER_EXT_JSON));
  }

  /** Row-level (pre-aggregation) rows from the direct {@code occurrence-identifier} link. */
  private static Optional<Dataset<Row>> buildDirectOccurrenceIdentifierRows(TableLoader loader) {
    Optional<Dataset<Row>> identifierDfOpt = loader.load(TABLE_OCCURRENCE_IDENTIFIER);
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");

    if (identifierDfOpt.isEmpty() || occurrenceDfOpt.isEmpty()) {
      log.debug(
          "Skipping direct occurrence-identifier rows: occurrence-identifier present={}, occurrence present={}",
          identifierDfOpt.isPresent(),
          occurrenceDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> resolved =
        resolveToParentId(
            identifierDfOpt.get(),
            "occurrence_fk",
            occurrenceDfOpt.get(),
            "occurrence_pk",
            "occurrenceID");
    return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
  }

  /** Resolved via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}; same column shape as {@link #buildDirectOccurrenceIdentifierRows}, safe to union. */
  private static Optional<Dataset<Row>> buildMaterialIdentifierRows(TableLoader loader) {
    Optional<Dataset<Row>> materialIdentifierDfOpt = loader.load(TABLE_MATERIAL_IDENTIFIER);
    if (materialIdentifierDfOpt.isEmpty()) {
      log.debug("No material-identifier table present; skipping material-identifier merge");
      return Optional.empty();
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug(
          "No single-material-per-occurrence links available; skipping material-identifier merge");
      return Optional.empty();
    }

    Dataset<Row> resolved =
        resolveToParentId(
            materialIdentifierDfOpt.get(),
            "materialEntity_fk",
            materialLinksOpt.get(),
            "materialEntity_pk",
            "occurrenceID");
    return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
  }

  /** Drops FK, parent PK, and any row whose FK didn't resolve — never leaves a null-keyed row for aggregation. */
  private static Dataset<Row> resolveToParentId(
      Dataset<Row> identifierDf,
      String fkColumn,
      Dataset<Row> parentDf,
      String parentPkColumn,
      String parentIdColumn) {
    return identifierDf
        .join(
            parentDf.select(parentPkColumn, parentIdColumn),
            identifierDf.col(fkColumn).equalTo(parentDf.col(parentPkColumn)),
            "left_outer")
        .drop(parentDf.col(parentPkColumn))
        .drop(identifierDf.col(fkColumn))
        .filter(functions.col(parentIdColumn).isNotNull());
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
}
