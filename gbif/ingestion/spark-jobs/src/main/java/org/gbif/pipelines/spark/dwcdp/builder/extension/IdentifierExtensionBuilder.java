package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds Identifier extension Datasets — {@code Extension.IDENTIFIER} ({@code
 * http://rs.gbif.org/terms/1.0/Identifier}) — for the occurrence-core and event-core paths. The
 * occurrence path merges {@code occurrence-identifier} (direct) with {@code material-identifier}
 * (via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}); the event path maps direct
 * {@code event-identifier} rows.
 *
 * <p><b>Two things are less certain here than for the other extensions built this session, and
 * worth being explicit about:</b>
 *
 * <ul>
 *   <li>{@code Extension.IDENTIFIER}'s row type is confirmed to exist, but there is no confirmed
 *       evidence (in anything reviewed so far) of a downstream interpreter that actually reads it —
 *       unlike Multimedia/eMoF/Humboldt/Identification, none of which had this gap.
 *   <li>The DwC-DP fields ({@code identifier}, {@code identifierType}, {@code identifierTypeIRI},
 *       {@code identifierTypeSource}, {@code identifierLanguage}) are passed through via the normal
 *       {@code TermResolver} machinery (TermFactory match, then raw-name fallback) rather than an
 *       invented rename scheme — there is no confirmed field-level mapping to verify against, the
 *       way there was for the media renames.
 * </ul>
 */
@Slf4j
public class IdentifierExtensionBuilder {

  public static final String TABLE_EVENT_IDENTIFIER = "event-identifier";
  static final String TABLE_OCCURRENCE_IDENTIFIER = "occurrence-identifier";
  static final String TABLE_MATERIAL_IDENTIFIER = "material-identifier";

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

  /**
   * Row-level (pre-aggregation) rows from {@code material-identifier}, resolved through {@link
   * MaterialJoinBuilder#singleMaterialOccurrenceLinks} down to the occurrence the material record
   * is exactly-one evidence for. Same output column shape as {@link
   * #buildDirectOccurrenceIdentifierRows} — both share the same generic {@code identifier}/{@code
   * identifierType}/{@code identifierTypeIRI}/{@code identifierTypeSource}/{@code
   * identifierLanguage} fields per the DwC-DP schema's shared "*-identifier" pattern — so safe to
   * union before aggregating.
   */
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

  /**
   * Resolves {@code identifierDf}'s surrogate FK to a natural parent identifier via {@code
   * parentDf}, dropping the FK and the parent's surrogate PK, and dropping any row whose FK didn't
   * resolve to a real parent — same null-drop policy {@link MediaExtensionBuilder}/{@link
   * AssertionExtensionBuilder} apply in their own resolution helpers, for the same reason: a
   * left-outer join alone would let such a row survive with a null key into the aggregation step.
   */
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
