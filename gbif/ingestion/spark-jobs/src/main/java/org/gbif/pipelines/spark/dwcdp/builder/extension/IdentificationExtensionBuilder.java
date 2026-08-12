package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Identification History extension — every {@code identification} row linked to an
 * occurrence, accepted or not, as its full re-identification history. Distinct from {@link
 * IdentificationJoinBuilder}, which flattens only the single current identification onto core
 * terms; both read the same rows independently.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>identification.occurrence_fk = occurrence.occurrence_pk (left outer) → history extension row
 * </ul>
 *
 * <p><b>Deferred:</b> {@code identification.materialEntity_fk} path — pending {@code material}'s
 * own extension/history work. See mapping doc §4.8.
 */
@Slf4j
public class IdentificationExtensionBuilder {

  public static final String TABLE_IDENTIFICATION = "identification";

  public static final String ROW_TYPE_IDENTIFICATION = Extension.IDENTIFICATION.getRowType();

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
