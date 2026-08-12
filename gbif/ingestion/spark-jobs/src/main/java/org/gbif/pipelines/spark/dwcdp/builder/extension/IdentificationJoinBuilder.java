package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Enriches occurrence rows with the taxonomic rank hierarchy from {@code identification} (feeds
 * {@code TaxonomyInterpreter}'s name-usage match confidence).
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>identification.occurrence_fk = occurrence.occurrence_pk (left outer), gated to exactly-one
 *       {@code isAcceptedIdentification = true} row
 * </ul>
 *
 * <p>Occurrence's own value always wins on overlapping fields; only fields occurrence lacks
 * entirely (principally the rank hierarchy) are added.
 *
 * <p><b>Deferred:</b> {@code identification.materialEntity_fk} path; {@code
 * identification-agent-role}, {@code identification-reference}. See mapping doc §4.8.
 */
@Slf4j
public class IdentificationJoinBuilder {

  public static final String TABLE_IDENTIFICATION = "identification";
  static final String OCCURRENCE_FK_COLUMN = "occurrence_fk";
  static final String OCCURRENCE_PK_COLUMN = "occurrence_pk";
  static final String IS_ACCEPTED_COLUMN = "isAcceptedIdentification";

  private static final Set<String> EXCLUDED_IDENTIFICATION_COLUMNS =
      Set.of(
          "identification_pk",
          "identificationProtocol_fk",
          "materialEntity_fk",
          "media_fk",
          "nucleotideAnalysis_fk",
          "nucleotideSequence_fk",
          OCCURRENCE_FK_COLUMN,
          "organism_fk",
          IS_ACCEPTED_COLUMN);

  private IdentificationJoinBuilder() {}

  /** {@code occurrenceDf} unchanged if identification is absent or missing needed columns. */
  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    Optional<Dataset<Row>> identificationDfOpt = loader.load(TABLE_IDENTIFICATION);
    if (identificationDfOpt.isEmpty()) {
      log.debug("No identification table present; skipping identification join");
      return occurrenceDf;
    }

    if (!Arrays.asList(occurrenceDf.columns()).contains(OCCURRENCE_PK_COLUMN)) {
      log.warn(
          "occurrence table has no {} column; skipping identification join", OCCURRENCE_PK_COLUMN);
      return occurrenceDf;
    }

    Dataset<Row> identificationDf = identificationDfOpt.get();
    List<String> identificationCols = Arrays.asList(identificationDf.columns());
    if (!identificationCols.contains(OCCURRENCE_FK_COLUMN)
        || !identificationCols.contains(IS_ACCEPTED_COLUMN)) {
      log.warn(
          "identification table missing {} or {} column; skipping identification join",
          OCCURRENCE_FK_COLUMN,
          IS_ACCEPTED_COLUMN);
      return occurrenceDf;
    }

    Dataset<Row> singleAccepted = singleAcceptedPerOccurrence(identificationDf);

    return join(occurrenceDf, singleAccepted);
  }

  /** Filters to {@code isAcceptedIdentification = true}, non-null {@code occurrence_fk}, keeps only groups of exactly one. */
  private static Dataset<Row> singleAcceptedPerOccurrence(Dataset<Row> identificationDf) {
    Dataset<Row> accepted =
        identificationDf
            .filter(functions.col(IS_ACCEPTED_COLUMN).equalTo(true))
            .filter(functions.col(OCCURRENCE_FK_COLUMN).isNotNull());

    Dataset<Row> singleLinkKeys =
        accepted
            .groupBy(functions.col(OCCURRENCE_FK_COLUMN))
            .count()
            .filter(functions.col("count").equalTo(1))
            .select(functions.col(OCCURRENCE_FK_COLUMN).as("__single_accepted_fk"));

    return accepted
        .join(
            singleLinkKeys,
            accepted.col(OCCURRENCE_FK_COLUMN).equalTo(singleLinkKeys.col("__single_accepted_fk")),
            "inner")
        .drop("__single_accepted_fk");
  }

  /** Pure join transform, occurrence value wins on overlap. */
  private static Dataset<Row> join(Dataset<Row> occurrenceDf, Dataset<Row> identificationDf) {
    Set<String> occurrenceCols = new HashSet<>(Arrays.asList(occurrenceDf.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : occurrenceDf.columns()) {
      selectCols.add(occurrenceDf.col(col));
    }
    for (String col : identificationDf.columns()) {
      if (!occurrenceCols.contains(col) && !EXCLUDED_IDENTIFICATION_COLUMNS.contains(col)) {
        selectCols.add(identificationDf.col(col));
        log.debug("Adding identification column '{}' to occurrence rows", col);
      }
    }

    Dataset<Row> joined =
        occurrenceDf
            .join(
                identificationDf,
                occurrenceDf
                    .col(OCCURRENCE_PK_COLUMN)
                    .equalTo(identificationDf.col(OCCURRENCE_FK_COLUMN)),
                "left_outer")
            .select(selectCols.toArray(new Column[0]));

    log.info(
        "Identification join complete: occurrence columns before={}, after={}",
        occurrenceDf.columns().length,
        joined.columns().length);

    return joined;
  }

  /** Funnels over identification rows, not candidates — genuine many-compete-for-one-slot ambiguity, like {@link MaterialJoinBuilder.MaterialFunnel}. Buckets: not accepted / accepted no FK / used / ambiguous (dropped). */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> identificationDfOpt = loader.load(TABLE_IDENTIFICATION);
    if (identificationDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> identificationDf = identificationDfOpt.get();
    List<String> cols = Arrays.asList(identificationDf.columns());
    if (!cols.contains(OCCURRENCE_FK_COLUMN) || !cols.contains(IS_ACCEPTED_COLUMN)) {
      return Optional.empty();
    }

    String label = "IdentificationJoinBuilder (occurrence taxonomic rank enrichment)";
    long total = identificationDf.count();

    Dataset<Row> accepted =
        identificationDf.filter(functions.col(IS_ACCEPTED_COLUMN).equalTo(true));
    long acceptedCount = accepted.count();
    long notAccepted = total - acceptedCount;

    long acceptedWithFkCount =
        accepted.filter(functions.col(OCCURRENCE_FK_COLUMN).isNotNull()).count();
    long acceptedNoFk = acceptedCount - acceptedWithFkCount;

    long usedForEnrichment = singleAcceptedPerOccurrence(identificationDf).count();
    long ambiguousDropped = acceptedWithFkCount - usedForEnrichment;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("identification rows (total)", total),
                new JoinFunnel.Bucket("not accepted, ignored", notAccepted),
                new JoinFunnel.Bucket("accepted, no occurrence_fk, ignored", acceptedNoFk),
                new JoinFunnel.Bucket(
                    "accepted with occurrence_fk, used for enrichment", usedForEnrichment),
                new JoinFunnel.Bucket(
                    "accepted with occurrence_fk, ambiguous (>1 per occurrence), DROPPED",
                    ambiguousDropped))));
  }
}
