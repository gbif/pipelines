package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Identification History extension Dataset — {@code Extension.IDENTIFICATION} ({@code
 * http://rs.tdwg.org/dwc/terms/Identification}) — holding every {@code identification} row linked
 * to an occurrence, accepted or not, as that occurrence's full re-identification history.
 *
 * <p>Distinct from {@link IdentificationJoinBuilder}, which flattens only the single current
 * identification ({@code isAcceptedIdentification = true}, exactly one) directly onto occurrence's
 * own core terms. Per project mapping notes, a current identification is "either flattened onto
 * Occurrence.identifiedBy/Occurrence.dateIdentified... or pushed to the Identification History
 * extension (if historical)" — this builder is the "pushed to history" half; {@link
 * IdentificationJoinBuilder} is the "flattened" half. Both read the same underlying {@code
 * identification} rows independently: a row that happens to be the sole accepted one is both
 * flattened onto occurrence core by that builder <em>and</em> still present in the full history
 * list built here, since the history is a complete audit trail, not just the superseded entries.
 *
 * <p>Only the {@code identification.occurrence_fk} link is handled here, same scope restriction as
 * {@link IdentificationJoinBuilder} — {@code materialEntity_fk}-linked identifications are deferred
 * pending {@code material}'s own extension/history work.
 */
@Slf4j
public class IdentificationExtensionBuilder {

  public static final String TABLE_IDENTIFICATION = "identification";

  /** Extension.IDENTIFICATION.getRowType(). */
  public static final String ROW_TYPE_IDENTIFICATION =
      "http://rs.tdwg.org/dwc/terms/Identification";

  public static final String COL_IDENTIFICATION_EXT_JSON = "identificationExtJson";

  static final String OCCURRENCE_FK_COLUMN = "occurrence_fk";
  static final String OCCURRENCE_PK_COLUMN = "occurrence_pk";

  /**
   * Internal surrogate keys on {@code identification} that have no business surviving into the
   * history extension's term maps — the join key itself, plus every other entity-link FK ({@code
   * identification} can attach to material/media/organism/nucleotide records too, one per row per
   * the schema, all irrelevant to an occurrence-scoped history).
   */
  private static final Set<String> EXCLUDED_IDENTIFICATION_COLUMNS =
      Set.of(
          "identification_pk",
          OCCURRENCE_FK_COLUMN,
          "materialEntity_fk",
          "media_fk",
          "organism_fk",
          "nucleotideAnalysis_fk",
          "nucleotideSequence_fk",
          "identificationProtocol_fk");

  private IdentificationExtensionBuilder() {}

  /**
   * Returns a two-column Dataset {@code (occurrenceID, identificationExtJson)}, or {@link
   * Optional#empty()} if {@code identification} or {@code occurrence} is absent, or {@code
   * identification} has no {@code occurrence_fk} column.
   */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> identificationDfOpt = loader.load(TABLE_IDENTIFICATION);
    if (identificationDfOpt.isEmpty()) {
      log.debug("No identification table present; skipping identification extension");
      return Optional.empty();
    }

    Dataset<Row> identificationDf = identificationDfOpt.get();
    if (!Arrays.asList(identificationDf.columns()).contains(OCCURRENCE_FK_COLUMN)) {
      log.debug(
          "identification table has no {} column; skipping identification extension",
          OCCURRENCE_FK_COLUMN);
      return Optional.empty();
    }

    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");
    if (occurrenceDfOpt.isEmpty()) {
      log.debug("No occurrence table present; skipping identification extension");
      return Optional.empty();
    }

    Dataset<Row> occurrenceDf = occurrenceDfOpt.get();
    Dataset<Row> withOccurrenceLink =
        identificationDf.filter(identificationDf.col(OCCURRENCE_FK_COLUMN).isNotNull());

    Dataset<Row> withOccurrenceId =
        withOccurrenceLink
            .join(
                occurrenceDf.select(OCCURRENCE_PK_COLUMN, "occurrenceID"),
                withOccurrenceLink
                    .col(OCCURRENCE_FK_COLUMN)
                    .equalTo(occurrenceDf.col(OCCURRENCE_PK_COLUMN)),
                "inner")
            .drop(occurrenceDf.col(OCCURRENCE_PK_COLUMN))
            .drop(withOccurrenceLink.col(OCCURRENCE_FK_COLUMN));

    for (String excluded : EXCLUDED_IDENTIFICATION_COLUMNS) {
      if (Arrays.asList(withOccurrenceId.columns()).contains(excluded)) {
        withOccurrenceId = withOccurrenceId.drop(excluded);
      }
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark,
            withOccurrenceId,
            withOccurrenceId.columns(),
            "occurrenceID",
            COL_IDENTIFICATION_EXT_JSON));
  }
}
