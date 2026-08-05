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
 * Enriches occurrence rows by left-joining the {@code identification} table onto them, bringing in
 * the taxonomic rank hierarchy — {@code kingdom}, {@code phylum}, {@code class}, {@code order},
 * {@code superfamily}, {@code family}, {@code subfamily}, {@code tribe}, {@code subtribe}, {@code
 * genus}, {@code genericName}, {@code subgenus}, {@code specificEpithet}, {@code
 * infraspecificEpithet} — that {@code occurrence} never carries on its own. {@code
 * org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter#createNameUsageMatchRequest} builds
 * its {@code NameUsageMatchRequest} with all of these fields currently null for every DwC-DP
 * occurrence, which measurably degrades match confidence — {@code checkFuzzy} specifically treats
 * an all-empty higher-taxa set as a signal to downgrade a {@code VARIANT} match down to {@code
 * NONE}.
 *
 * <p><b>Only the {@code identification.occurrence_fk} path is handled here.</b> {@code
 * identification} also carries {@code materialEntity_fk}/{@code media_fk}/{@code organism_fk}/
 * nucleotide FKs — per the schema, only one of these is populated on a given row depending on what
 * the identification is actually about — but {@code material} isn't joined anywhere in this
 * pipeline yet, so a material-sourced identification has nothing on the occurrence side to attach
 * to regardless. That path is naturally bundled into whenever {@code material} itself gets joined.
 *
 * <p><b>Enrichment only applies when an occurrence has exactly one {@code identification} row with
 * {@code isAcceptedIdentification = true}.</b> Zero accepted rows (nothing to enrich from) or more
 * than one (a data-quality issue the schema doesn't prevent — re-identification history without a
 * single clear "current" row) both leave the occurrence unenriched entirely, rather than guessing
 * at a tie-break; its own existing {@code scientificName}/{@code taxonID}/etc. fields remain the
 * only source.
 *
 * <p>Columns already present on {@code occurrence} are never overwritten by identification's copy —
 * same "occurrence value wins" precedence {@link OrganismJoinBuilder} already applies. Only fields
 * occurrence doesn't have at all (principally the rank hierarchy) are added. Internal surrogate
 * keys ({@code identification_pk}, every {@code *_fk} column on {@code identification}, and {@code
 * isAcceptedIdentification} itself, which was only a filter criterion) are excluded from what gets
 * added.
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

  /**
   * Returns {@code occurrenceDf} enriched with identification fields not already present on it, for
   * occurrences with exactly one accepted identification, or the original {@code occurrenceDf}
   * unchanged if the {@code identification} table is absent, occurrence has no {@code
   * occurrence_pk} column, or identification is missing the columns this join needs.
   */
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

  /**
   * Filters {@code identificationDf} to {@code isAcceptedIdentification = true} rows with a
   * non-null {@code occurrence_fk}, then keeps only {@code occurrence_fk} groups with <em>exactly
   * one</em> such row.
   */
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

  /**
   * Left-joins the (already filtered to exactly-one-accepted-per-occurrence) identification rows
   * onto occurrence via {@code occurrence_fk -> occurrence_pk}, adding only columns occurrence
   * doesn't already carry — same column-precedence policy as {@link
   * OrganismJoinBuilder#joinOrganism}.
   */
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

  /**
   * Computes a {@link JoinFunnel} breakdown of {@code identification} rows, mirroring {@link
   * #enrichOccurrences}'s decision logic — reuses {@link #singleAcceptedPerOccurrence} directly so
   * the two can't drift apart. Unlike the other join builders in this commit, this one has genuine
   * many-rows-compete-for-one-slot ambiguity (like {@link MaterialJoinBuilder.MaterialFunnel}), so
   * it funnels over {@code identification} rows rather than a single candidates/resolved/unresolved
   * split. Buckets are mutually exclusive and sum to the total row count:
   *
   * <ul>
   *   <li><b>not accepted, ignored</b> — {@code isAcceptedIdentification != true}
   *   <li><b>accepted, no occurrence_fk, ignored</b> — accepted but not linked to any occurrence
   *   <li><b>accepted with occurrence_fk, used for enrichment</b> — the sole accepted
   *       identification for its occurrence; that occurrence's rank hierarchy gets filled in
   *   <li><b>accepted with occurrence_fk, ambiguous (&gt;1 per occurrence), DROPPED</b> — more than
   *       one accepted identification links to the same occurrence, so per {@link
   *       #singleAcceptedPerOccurrence} none of them are used — the occurrence is left unenriched
   *       rather than the join guessing at a tie-break
   * </ul>
   *
   * @return empty if {@code identification} is absent, or missing {@code occurrence_fk} or {@code
   *     isAcceptedIdentification} entirely
   */
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
