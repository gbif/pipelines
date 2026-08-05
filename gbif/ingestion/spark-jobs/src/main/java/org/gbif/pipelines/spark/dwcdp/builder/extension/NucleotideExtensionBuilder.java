package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds DNA Derived Data extension Datasets — {@code Extension.DNA_DERIVED_DATA} — from DwC-DP's
 * {@code nucleotide-analysis}, {@code nucleotide-sequence}, and {@code molecular-protocol} tables.
 *
 * <p><b>Join shape.</b> {@code nucleotide-analysis} is mostly a join row: the DNA sequence itself
 * lives on {@code nucleotide-sequence} ({@code nucleotideSequence_fk}, required), and the bulk of
 * the extension's real content — the MIxS-shaped method/protocol fields ({@code target_gene},
 * {@code pcr_primers}, {@code seq_meth}, {@code nucl_acid_amp}, ...) — lives on {@code
 * molecular-protocol} ({@code molecularProtocol_fk}, required). Both are resolved via {@code
 * left_outer} rather than {@code inner} — required-per-profile is not the same guarantee as
 * present-in-this-package, and a row with a dangling FK should still surface with whatever it does
 * have rather than vanish.
 *
 * <p><b>Term resolution.</b> Confirmed via the {@code dwc-api} javadoc (enumerated constants of
 * {@code MixsTerm}/{@code GbifDnaTerm}/{@code GgbnTerm}): almost every {@code molecular-protocol}
 * column matches a registered term's simple name exactly and resolves correctly through the
 * ordinary {@code TermFactory} path with no rename needed. Three confirmed exceptions are handled
 * in {@link DwcDpTermMappings}: {@code nucleotide-sequence.sequence} resolves via {@code
 * TermFactory} to the real but wrong term {@code ggbn:sequence} rather than the {@code
 * gbif:dna_sequence} the DNA Derived Data interpreter actually reads (same "resolves, just to the
 * wrong term" shape as {@code accessURI}), and {@code molecular-protocol.single_cell_lysis_appr}/
 * {@code single_cell_lysis_prot} use DwC-DP's own naming rather than {@code MixsTerm}'s abbreviated
 * {@code sc_lysis_approach}/{@code sc_lysis_method} constants. Everything else — including the
 * qPCR/MIQE-shaped fields ({@code concentration}, {@code ratioOfAbsorbance260_230}, {@code
 * methodDeterminationConcentrationAndRatios}, ...) and the {@code pcr_primer_*} fields — matches a
 * registered term already and needed no new entry.
 *
 * <p><b>Attachment: two mutually exclusive paths, mirroring the two optional FKs {@code
 * nucleotide-analysis} actually carries ({@code event_fk}, {@code materialEntity_fk}) — no
 * synthetic/virtual record is manufactured the way {@link MaterialJoinBuilder} does for material
 * without an evidence occurrence; DNA_DERIVED_DATA is an extension, not a core, so it only ever
 * needs a valid join key, and one already exists on the row itself:</b>
 *
 * <ul>
 *   <li>{@link #buildOccurrence} — {@code materialEntity_fk} populated: resolved down to the
 *       occurrence it's exactly-one evidence for via {@link
 *       MaterialJoinBuilder#singleMaterialOccurrenceLinks} (real or virtual occurrence, whichever
 *       material already resolved to — reused as-is, not re-derived). This is the physical-specimen
 *       path (e.g. tissue sample sequenced from a voucher).
 *   <li>{@link #buildEvent} — {@code event_fk} populated, {@code materialEntity_fk} absent:
 *       resolved directly to the event via {@code event_fk → event.event_pk → eventID}, the same
 *       pattern {@link HumboldtExtensionBuilder} uses for {@code survey}. This is the
 *       eDNA/metabarcoding path — a water/soil/air sample sequenced with no physical specimen ever
 *       accessioned. GBIF's own DNA-derived-data guidance treats event-level attachment as a
 *       first-class case, not a fallback: "eDNA and DNA derived data is linked to occurrence data
 *       with the use of occurrenceID and/or eventID."
 * </ul>
 *
 * <p>A row with <em>both</em> FKs populated is attached only via {@link #buildOccurrence} — {@link
 * #buildEvent} explicitly excludes rows where {@code materialEntity_fk} is also present — so the
 * same analysis never appears twice (once nested under its occurrence, once again directly on the
 * event) in the event-core output.
 *
 * <p><b>Not yet handled</b> (separate, deferred work, same as {@code material-identifier}/{@code
 * material-provenance} on {@link MaterialJoinBuilder}): {@code nucleotide-analysis-assertion} and
 * {@code molecular-protocol-assertion} (eMoF-style facts about a specific analysis or protocol —
 * would need their own aggregation, unioned in carefully rather than joined flat, to avoid a
 * cartesian fan-out against each other), and {@code molecular-protocol-agent-role}/{@code
 * molecular-protocol-reference} (no confirmed DwC-A field for a protocol's performing agent, same
 * open question as the general Agent-role gap). {@code identification.nucleotideAnalysis_fk}/{@code
 * nucleotideSequence_fk} (an identification made <em>from</em> a DNA analysis) are also untouched —
 * that is provenance on the identification, not part of this extension.
 */
@Slf4j
public class NucleotideExtensionBuilder {

  static final String TABLE_NUCLEOTIDE_ANALYSIS = "nucleotide-analysis";
  static final String TABLE_NUCLEOTIDE_SEQUENCE = "nucleotide-sequence";
  static final String TABLE_MOLECULAR_PROTOCOL = "molecular-protocol";

  /** Extension.DNA_DERIVED_DATA.getRowType() — derived from the real gbif-api enum. */
  public static final String ROW_TYPE_DNA_DERIVED_DATA = Extension.DNA_DERIVED_DATA.getRowType();

  public static final String COL_DNA_EXT_JSON = "dnaExtJson";

  private static final String NUCLEOTIDE_ANALYSIS_PK_COLUMN = "nucleotideAnalysis_pk";
  private static final String NUCLEOTIDE_SEQUENCE_FK_COLUMN = "nucleotideSequence_fk";
  private static final String NUCLEOTIDE_SEQUENCE_PK_COLUMN = "nucleotideSequence_pk";
  private static final String MOLECULAR_PROTOCOL_FK_COLUMN = "molecularProtocol_fk";
  private static final String MOLECULAR_PROTOCOL_PK_COLUMN = "molecularProtocol_pk";
  private static final String EVENT_FK_COLUMN = "event_fk";
  private static final String MATERIAL_ENTITY_FK_COLUMN = "materialEntity_fk";
  private static final String MATERIAL_ENTITY_PK_COLUMN = "materialEntity_pk";

  private NucleotideExtensionBuilder() {}

  /**
   * Returns a two-column Dataset {@code (eventID, dnaExtJson)} for {@code nucleotide-analysis} rows
   * with a populated {@code event_fk} and no {@code materialEntity_fk} (the eDNA/metabarcoding
   * path). Returns {@link Optional#empty()} when {@code nucleotide-analysis} or {@code event} is
   * absent, {@code nucleotide-analysis} has no {@code event_fk} column, or no row resolves.
   */
  public static Optional<Dataset<Row>> buildEvent(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> payloadOpt = resolveAnalysisPayload(loader);
    if (payloadOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> payload = payloadOpt.get();
    List<String> columns = Arrays.asList(payload.columns());
    if (!columns.contains(EVENT_FK_COLUMN)) {
      log.debug("nucleotide-analysis has no event_fk column; skipping event-level DNA extension");
      return Optional.empty();
    }

    boolean hasMaterialFk = columns.contains(MATERIAL_ENTITY_FK_COLUMN);
    Dataset<Row> eventLinked =
        hasMaterialFk
            ? payload.filter(
                functions
                    .col(EVENT_FK_COLUMN)
                    .isNotNull()
                    .and(functions.col(MATERIAL_ENTITY_FK_COLUMN).isNull()))
            : payload.filter(functions.col(EVENT_FK_COLUMN).isNotNull());

    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()
        || !Arrays.asList(eventDfOpt.get().columns()).contains("eventID")
        || !Arrays.asList(eventDfOpt.get().columns()).contains("event_pk")) {
      log.debug(
          "event table absent or missing eventID/event_pk; skipping event-level DNA extension");
      return Optional.empty();
    }
    Dataset<Row> eventDf = eventDfOpt.get();

    Dataset<Row> resolved =
        eventLinked
            .join(
                eventDf.select("event_pk", "eventID"),
                eventLinked.col(EVENT_FK_COLUMN).equalTo(eventDf.col("event_pk")),
                "inner")
            .drop(eventDf.col("event_pk"))
            .drop(eventLinked.col(EVENT_FK_COLUMN));
    if (hasMaterialFk) {
      resolved = resolved.drop(MATERIAL_ENTITY_FK_COLUMN);
    }

    if (resolved.isEmpty()) {
      log.debug("No nucleotide-analysis rows resolved to an event; skipping DNA extension");
      return Optional.empty();
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, resolved, resolved.columns(), "eventID", COL_DNA_EXT_JSON));
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, dnaExtJson)} for {@code nucleotide-analysis}
   * rows with a populated {@code materialEntity_fk} that resolves to a single unambiguous
   * occurrence via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks} (the physical-specimen
   * path). Returns {@link Optional#empty()} when {@code nucleotide-analysis} is absent, has no
   * {@code materialEntity_fk} column, or no material-linked-occurrence resolution is available.
   */
  public static Optional<Dataset<Row>> buildOccurrence(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> payloadOpt = resolveAnalysisPayload(loader);
    if (payloadOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> payload = payloadOpt.get();
    if (!Arrays.asList(payload.columns()).contains(MATERIAL_ENTITY_FK_COLUMN)) {
      log.debug(
          "nucleotide-analysis has no materialEntity_fk column; skipping occurrence-level DNA "
              + "extension");
      return Optional.empty();
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug(
          "No single-material-per-occurrence links available; skipping occurrence-level DNA "
              + "extension");
      return Optional.empty();
    }
    Dataset<Row> materialLinks = materialLinksOpt.get();

    Dataset<Row> materialLinked =
        payload.filter(functions.col(MATERIAL_ENTITY_FK_COLUMN).isNotNull());
    Dataset<Row> resolved =
        materialLinked
            .join(
                materialLinks,
                materialLinked
                    .col(MATERIAL_ENTITY_FK_COLUMN)
                    .equalTo(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN)),
                "inner")
            .drop(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN))
            .drop(materialLinked.col(MATERIAL_ENTITY_FK_COLUMN));
    if (Arrays.asList(resolved.columns()).contains(EVENT_FK_COLUMN)) {
      resolved = resolved.drop(EVENT_FK_COLUMN);
    }

    if (resolved.isEmpty()) {
      log.debug(
          "No nucleotide-analysis rows resolved to a single-material occurrence; skipping DNA "
              + "extension");
      return Optional.empty();
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, resolved, resolved.columns(), "occurrenceID", COL_DNA_EXT_JSON));
  }

  /**
   * Loads {@code nucleotide-analysis} and left-joins in {@code nucleotide-sequence} (via {@code
   * nucleotideSequence_fk}) and {@code molecular-protocol} (via {@code molecularProtocol_fk}),
   * dropping each side's surrogate PK/FK pair once resolved. {@code event_fk} and {@code
   * materialEntity_fk} are deliberately retained here — each of {@link #buildEvent}/{@link
   * #buildOccurrence} needs them to pick its own subset before finally dropping them itself.
   *
   * @return {@link Optional#empty()} only if {@code nucleotide-analysis} itself is absent.
   */
  private static Optional<Dataset<Row>> resolveAnalysisPayload(TableLoader loader) {
    Optional<Dataset<Row>> analysisDfOpt = loader.load(TABLE_NUCLEOTIDE_ANALYSIS);
    if (analysisDfOpt.isEmpty()) {
      log.debug("No nucleotide-analysis table present; skipping DNA extension entirely");
      return Optional.empty();
    }

    Dataset<Row> withSequence =
        leftJoinDropFk(
            loader,
            analysisDfOpt.get(),
            TABLE_NUCLEOTIDE_SEQUENCE,
            NUCLEOTIDE_SEQUENCE_FK_COLUMN,
            NUCLEOTIDE_SEQUENCE_PK_COLUMN);
    Dataset<Row> withProtocol =
        leftJoinDropFk(
            loader,
            withSequence,
            TABLE_MOLECULAR_PROTOCOL,
            MOLECULAR_PROTOCOL_FK_COLUMN,
            MOLECULAR_PROTOCOL_PK_COLUMN);

    return Optional.of(withProtocol.drop(NUCLEOTIDE_ANALYSIS_PK_COLUMN));
  }

  /**
   * Left-joins {@code childTable} onto {@code left} via {@code fkColumn → childPkColumn}, dropping
   * both the join columns and the FK afterward. If {@code childTable} is absent, or {@code left}
   * has no {@code fkColumn} to begin with, {@code fkColumn} is simply dropped (unlike {@link
   * ProtocolJoinBuilder}'s raw-FK fallback, there is no DwC term this surrogate ID could
   * meaningfully stand in for on its own, so keeping it would only leak a meaningless internal
   * value).
   */
  private static Dataset<Row> leftJoinDropFk(
      TableLoader loader,
      Dataset<Row> left,
      String childTable,
      String fkColumn,
      String childPkColumn) {
    if (!Arrays.asList(left.columns()).contains(fkColumn)) {
      return left;
    }

    Optional<Dataset<Row>> childDfOpt = loader.load(childTable);
    if (childDfOpt.isEmpty()) {
      log.debug("No {} table present; dropping unresolved {}", childTable, fkColumn);
      return left.drop(fkColumn);
    }

    Dataset<Row> childDf = childDfOpt.get();
    if (!Arrays.asList(childDf.columns()).contains(childPkColumn)) {
      log.warn(
          "{} table is missing {}; dropping unresolved {}", childTable, childPkColumn, fkColumn);
      return left.drop(fkColumn);
    }

    return left.join(childDf, left.col(fkColumn).equalTo(childDf.col(childPkColumn)), "left_outer")
        .drop(childDf.col(childPkColumn))
        .drop(left.col(fkColumn));
  }
}
