package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds Identifier extension Datasets — {@code Extension.IDENTIFIER} ({@code
 * http://rs.gbif.org/terms/1.0/Identifier}) — for the occurrence-core path, merging {@code
 * occurrence-identifier} (direct) with {@code material-identifier} (via {@link
 * MaterialJoinBuilder#singleMaterialOccurrenceLinks}) into one extension, same architecture and
 * same reasoning as {@link MediaExtensionBuilder}/{@link AssertionExtensionBuilder}'s equivalent
 * merges: once {@link MaterialJoinBuilder} has established a 1:1 occurrence/material relationship,
 * an alternate identifier for the specimen and one for the occurrence both just identify the same
 * real-world thing.
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
 *
 * <p>{@code event-identifier} is a separate, still-unbuilt gap — it attaches to {@code event}, not
 * {@code material}/{@code occurrence}, so it isn't part of this merge.
 */
@Slf4j
public class IdentifierExtensionBuilder {

  static final String TABLE_OCCURRENCE_IDENTIFIER = "occurrence-identifier";
  static final String TABLE_MATERIAL_IDENTIFIER = "material-identifier";

  /** Extension.IDENTIFIER.getRowType(). */
  public static final String ROW_TYPE_IDENTIFIER = "http://rs.gbif.org/terms/1.0/Identifier";

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
        resolveToOccurrenceId(
            identifierDfOpt.get(), "occurrence_fk", occurrenceDfOpt.get(), "occurrence_pk");
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
        resolveToOccurrenceId(
            materialIdentifierDfOpt.get(),
            "materialEntity_fk",
            materialLinksOpt.get(),
            "materialEntity_pk");
    return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
  }

  /**
   * Resolves {@code identifierDf}'s surrogate FK to {@code occurrenceID} via {@code parentDf},
   * dropping the FK and the parent's surrogate PK, and dropping any row whose FK didn't resolve to
   * a real parent — same null-drop policy {@link MediaExtensionBuilder}/{@link
   * AssertionExtensionBuilder} apply in their own resolution helpers, for the same reason: a
   * left-outer join alone would let such a row survive with a null key into the aggregation step.
   */
  private static Dataset<Row> resolveToOccurrenceId(
      Dataset<Row> identifierDf, String fkColumn, Dataset<Row> parentDf, String parentPkColumn) {
    return identifierDf
        .join(
            parentDf.select(parentPkColumn, "occurrenceID"),
            identifierDf.col(fkColumn).equalTo(parentDf.col(parentPkColumn)),
            "left_outer")
        .drop(parentDf.col(parentPkColumn))
        .drop(identifierDf.col(fkColumn))
        .filter(functions.col("occurrenceID").isNotNull());
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
