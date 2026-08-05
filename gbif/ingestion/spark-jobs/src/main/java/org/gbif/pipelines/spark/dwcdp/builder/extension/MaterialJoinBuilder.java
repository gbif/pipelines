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
 * Enriches occurrence rows by left-joining the {@code material} table onto them, bringing in
 * institution/collection/specimen fields occurrence never carries on its own — notably {@code
 * institutionCode}, {@code institutionID}, {@code ownerInstitutionCode}, {@code collectionCode},
 * {@code collectionID}, {@code catalogNumber}, {@code otherCatalogNumbers}, {@code preparations},
 * {@code typeStatus}, {@code disposition}. These fields sit behind two real gaps in the downstream
 * DwC-A interpretation without this join:
 *
 * <ul>
 *   <li>{@code org.gbif.pipelines.core.interpreters.core.GrscicollInterpreter} reads {@code
 *       institutionID}/{@code institutionCode}/{@code ownerInstitutionCode}/{@code collectionID}/
 *       {@code collectionCode} straight off the record — all null for DwC-DP occurrences without
 *       this join, so GrSciColl institution/collection matching never fires.
 *   <li>{@code org.gbif.pipelines.core.interpreters.specific.GbifIdInterpreter}'s triplet-based
 *       GBIF ID path builds its key from {@code institutionCode} + {@code collectionCode} + {@code
 *       catalogNumber} — unavailable for datasets that rely on the triplet rather than {@code
 *       occurrenceID} for identity, without this join.
 * </ul>
 *
 * <p><b>Join shape differs from every other builder in this package:</b> {@code
 * material.evidenceForOccurrenceID} is a <em>weak</em> FK straight to {@code
 * occurrence.occurrenceID} — both natural keys, no surrogate {@code _pk}/{@code _fk} resolution
 * needed, same shape as {@link GeologicalContextJoinBuilder}'s natural-to-natural join.
 *
 * <p><b>Enrichment only applies when an occurrence has exactly one material row citing it as
 * evidence.</b> Zero (nothing to enrich from) or more than one (a specimen + a separately
 * accessioned tissue sample, say — a real, valid scenario the schema explicitly allows) both leave
 * the occurrence unenriched, rather than guessing at a tie-break — same rule applied to {@link
 * IdentificationJoinBuilder} for the analogous exactly-one-accepted-identification case.
 *
 * <p>Columns already present on {@code occurrence} are never overwritten by material's copy — same
 * "occurrence value wins" precedence {@link OrganismJoinBuilder} already applies (material and
 * occurrence overlap on several fields: {@code identifiedBy}, {@code dateIdentified}, {@code
 * taxonID}, {@code scientificName}, etc.). Internal surrogate keys ({@code materialEntity_pk},
 * {@code collectionEvent_fk}, {@code derivationEvent_fk}, {@code provenance_fk}, {@code
 * usagePolicy_fk}) and the join key itself are excluded from what gets added.
 *
 * <p>Before the exactly-one filtering, {@code material} is also enriched with {@code
 * license}/{@code rightsHolder} from {@code usage-policy} via {@link UsagePolicyJoinBuilder#enrich}
 * — the same generic helper originally written for {@code media} — so those two fields flow through
 * onto occurrence via the ordinary column-bring-in logic below with no separate wiring needed.
 *
 * <p>Only the direct {@code material} → {@code occurrence} flat-field link is handled in this
 * class. {@code material-media} and {@code material-assertion} are handled separately, merged into
 * occurrence's own Multimedia/eMoF extensions by {@link MediaExtensionBuilder} and {@link
 * AssertionExtensionBuilder} respectively — see {@link #singleMaterialOccurrenceLinks}, which both
 * reuse. {@code material}'s remaining sub-tables ({@code material-identifier}, {@code
 * material-provenance}, {@code material-usage-policy}) and its links to {@code event} ({@code
 * collectionEvent_fk}, {@code derivationEvent_fk}) are still not handled — separate, later work,
 * same deferral pattern as {@code creator} on media.
 */
@Slf4j
public class MaterialJoinBuilder {

  public static final String TABLE_MATERIAL = "material";
  private static final String TABLE_OCCURRENCE = "occurrence";
  private static final String OCCURRENCE_ID_COLUMN = "occurrenceID";
  static final String EVIDENCE_FOR_OCCURRENCE_ID_COLUMN = "evidenceForOccurrenceID";
  private static final String MATERIAL_ENTITY_PK_COLUMN = "materialEntity_pk";
  private static final String MATERIAL_ENTITY_ID_COLUMN = "materialEntityID";
  private static final String COLLECTION_EVENT_FK_COLUMN = "collectionEvent_fk";
  private static final String VIRTUAL_OCCURRENCE_ID_PREFIX = "urn:gbif:dwcdp:material:";

  private static final Set<String> EXCLUDED_MATERIAL_COLUMNS =
      Set.of(
          "materialEntity_pk",
          EVIDENCE_FOR_OCCURRENCE_ID_COLUMN,
          "collectionEvent_fk",
          "derivationEvent_fk",
          "provenance_fk",
          "usagePolicy_fk");

  private MaterialJoinBuilder() {}

  /**
   * Left-anti-joins {@code materialDf} against the local {@code occurrence} table's {@code
   * occurrenceID} (when one is present), returning the rows whose {@code evidenceForOccurrenceID}
   * does <em>not</em> resolve to a real local occurrence.
   *
   * <p>This includes rows with a null {@code evidenceForOccurrenceID} (which can never match
   * anything) and rows whose value legitimately references an occurrence outside this package —
   * {@code evidenceForOccurrenceID} is a weak foreign key in the DwC-DP spec, not required to
   * resolve locally. If there is no local {@code occurrence} table (or it has no {@code
   * occurrenceID} column) at all, nothing can possibly resolve, so every row is returned unchanged.
   *
   * <p>Used by {@link #virtualMaterialOccurrences} to decide eligibility, and by {@link
   * #singleMaterialOccurrenceLinks} (via its complement, {@link #withEvidenceResolvedLocally}) to
   * keep evidence-based and virtual occurrence links mutually exclusive — a single source of truth
   * for "does this reference resolve locally" so the two can't drift apart.
   */
  private static Dataset<Row> withoutLocallyResolvedEvidence(
      TableLoader loader, Dataset<Row> materialDf) {
    Optional<Dataset<Row>> localOccurrenceIds = localOccurrenceIds(loader);
    if (localOccurrenceIds.isEmpty()) {
      return materialDf;
    }
    return materialDf.join(
        localOccurrenceIds.get(),
        materialDf
            .col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)
            .equalTo(localOccurrenceIds.get().col("__local_occurrence_id")),
        "left_anti");
  }

  /**
   * The complement of {@link #withoutLocallyResolvedEvidence}: rows whose {@code
   * evidenceForOccurrenceID} <em>does</em> resolve to a real local occurrence. Returns an empty
   * (zero-row, correctly-schema'd) dataset if there is no local {@code occurrence} table to resolve
   * against — nothing can resolve in that case, matching {@link #withoutLocallyResolvedEvidence}
   * returning everything unchanged.
   */
  private static Dataset<Row> withEvidenceResolvedLocally(
      TableLoader loader, Dataset<Row> materialDf) {
    Optional<Dataset<Row>> localOccurrenceIds = localOccurrenceIds(loader);
    if (localOccurrenceIds.isEmpty()) {
      return materialDf.filter(functions.lit(false));
    }
    return materialDf.join(
        localOccurrenceIds.get(),
        materialDf
            .col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)
            .equalTo(localOccurrenceIds.get().col("__local_occurrence_id")),
        "left_semi");
  }

  private static Optional<Dataset<Row>> localOccurrenceIds(TableLoader loader) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load(TABLE_OCCURRENCE);
    if (occurrenceDfOpt.isEmpty()
        || !Arrays.asList(occurrenceDfOpt.get().columns()).contains(OCCURRENCE_ID_COLUMN)) {
      return Optional.empty();
    }
    return Optional.of(
        occurrenceDfOpt
            .get()
            .select(functions.col(OCCURRENCE_ID_COLUMN).as("__local_occurrence_id"))
            .distinct());
  }

  /**
   * Returns {@code occurrenceDf} enriched with material fields not already present on it, for
   * occurrences with exactly one material row citing them as evidence, or the original {@code
   * occurrenceDf} unchanged if the {@code material} table is absent, occurrence has no {@code
   * occurrenceID} column, or material has no {@code evidenceForOccurrenceID} column.
   */
  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    Optional<Dataset<Row>> materialDfOpt = loader.load(TABLE_MATERIAL);
    if (materialDfOpt.isEmpty()) {
      log.debug("No material table present; skipping material join");
      return occurrenceDf;
    }

    if (!Arrays.asList(occurrenceDf.columns()).contains("occurrenceID")) {
      log.warn("occurrence table has no occurrenceID column; skipping material join");
      return occurrenceDf;
    }

    Dataset<Row> materialDf = materialDfOpt.get();
    if (!Arrays.asList(materialDf.columns()).contains(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)) {
      log.warn(
          "material table has no {} column; skipping material join",
          EVIDENCE_FOR_OCCURRENCE_ID_COLUMN);
      return occurrenceDf;
    }

    // license/rightsHolder live only on usage-policy, never on material itself — enrich before
    // the exactly-one filtering and join so they flow through the existing generic "bring in
    // whatever material columns occurrence doesn't already have" logic in join() below, without
    // needing separate wiring. usagePolicy_fk is dropped internally by
    // UsagePolicyJoinBuilder.enrich itself, so EXCLUDED_MATERIAL_COLUMNS' entry for it is a
    // harmless no-op from this point on rather than doing double duty.
    materialDf = UsagePolicyJoinBuilder.enrich(loader, materialDf);

    Dataset<Row> singleMaterial = singleMaterialPerOccurrence(materialDf);

    return join(occurrenceDf, singleMaterial);
  }

  /**
   * Filters {@code materialDf} to rows with a non-null {@code evidenceForOccurrenceID}, then keeps
   * only {@code evidenceForOccurrenceID} groups with <em>exactly one</em> such row.
   *
   * <p>Package-private rather than private: {@link MediaExtensionBuilder} and {@link
   * AssertionExtensionBuilder} reuse this directly via {@link #singleMaterialOccurrenceLinks} to
   * merge {@code material-media}/{@code material-assertion} into occurrence's own Multimedia/eMoF
   * extensions, applying the identical exactly-one-material rule rather than a second copy of it.
   */
  static Dataset<Row> singleMaterialPerOccurrence(Dataset<Row> materialDf) {
    Dataset<Row> withEvidence =
        materialDf.filter(functions.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN).isNotNull());

    Dataset<Row> singleLinkKeys =
        withEvidence
            .groupBy(functions.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN))
            .count()
            .filter(functions.col("count").equalTo(1))
            .select(functions.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN).as("__single_material_key"));

    return withEvidence
        .join(
            singleLinkKeys,
            withEvidence
                .col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)
                .equalTo(singleLinkKeys.col("__single_material_key")),
            "inner")
        .drop("__single_material_key");
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, materialEntity_pk)} — one row per occurrence
   * with exactly one material record citing it as evidence — for other builders that need to
   * resolve a material-side surrogate FK (e.g. {@code material-media.materialEntity_fk}, {@code
   * material-assertion.materialEntity_fk}) down to the occurrence it ultimately belongs to, using
   * this same exactly-one-material rule rather than a separate, potentially inconsistent one.
   *
   * <p>Returns {@link Optional#empty()} if {@code material} or {@code occurrence} is absent, {@code
   * material} is missing {@code evidenceForOccurrenceID} or {@code materialEntity_pk}, or —
   * critically — if no occurrence actually has an unambiguous single material link at all (e.g.
   * every occurrence in the package has zero or multiple material rows citing it): an empty result
   * here must look identical to "material absent" to every caller, or a left-outer join against a
   * genuinely zero-row Dataset can silently produce a null-keyed row downstream instead of
   * correctly contributing nothing.
   */
  public static Optional<Dataset<Row>> singleMaterialOccurrenceLinks(TableLoader loader) {
    Optional<Dataset<Row>> materialDfOpt = loader.load(TABLE_MATERIAL);
    if (materialDfOpt.isEmpty()) {
      log.debug("No material table present; skipping material-linked occurrence resolution");
      return Optional.empty();
    }

    Dataset<Row> materialDf = materialDfOpt.get();
    List<String> materialCols = Arrays.asList(materialDf.columns());
    if (!materialCols.contains(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)
        || !materialCols.contains("materialEntity_pk")) {
      log.debug(
          "material table missing {} or materialEntity_pk column; skipping material-linked "
              + "occurrence resolution",
          EVIDENCE_FOR_OCCURRENCE_ID_COLUMN);
      return Optional.empty();
    }

    // Restrict to evidence that resolves to a real local occurrence — evidence pointing outside
    // this package is already covered by virtualLinks below (see virtualMaterialOccurrences), so
    // including it here too would attach the same material's children under two different,
    // non-matching occurrenceIDs.
    Dataset<Row> single =
        withEvidenceResolvedLocally(loader, singleMaterialPerOccurrence(materialDf));
    Optional<Dataset<Row>> virtualLinks = virtualMaterialOccurrenceLinks(loader);
    if (single.isEmpty() && virtualLinks.isEmpty()) {
      log.debug(
          "No occurrence has an unambiguous single material link; skipping material-linked "
              + "occurrence resolution entirely");
      return Optional.empty();
    }

    Optional<Dataset<Row>> evidenceLinks =
        single.isEmpty()
            ? Optional.empty()
            : Optional.of(
                single.select(
                    functions.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN).as("occurrenceID"),
                    functions.col(MATERIAL_ENTITY_PK_COLUMN)));
    if (evidenceLinks.isEmpty()) {
      return virtualLinks.map(links -> links.drop("eventID"));
    }
    if (virtualLinks.isEmpty()) {
      return evidenceLinks;
    }
    return Optional.of(evidenceLinks.get().unionByName(virtualLinks.get().drop("eventID")));
  }

  /**
   * Builds event-core occurrence rows for material records whose {@code evidenceForOccurrenceID}
   * doesn't resolve to a real local occurrence — either because it's null, or because it references
   * an occurrence outside this package (a legitimate case: {@code evidenceForOccurrenceID} is a
   * weak foreign key in the DwC-DP spec, not required to resolve locally) — and which do have a
   * resolvable collection event. Their material entity identifier is reused as the occurrence
   * identifier when available; the stable material surrogate key supplies the fallback.
   */
  public static Optional<Dataset<Row>> virtualMaterialOccurrences(TableLoader loader) {
    Optional<Dataset<Row>> materialDfOpt = loader.load(TABLE_MATERIAL);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (materialDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> materialDf = materialDfOpt.get();
    Dataset<Row> eventDf = eventDfOpt.get();
    List<String> materialColumns = Arrays.asList(materialDf.columns());
    if (!materialColumns.contains(MATERIAL_ENTITY_PK_COLUMN)
        || !materialColumns.contains(COLLECTION_EVENT_FK_COLUMN)
        || !Arrays.asList(eventDf.columns()).contains("event_pk")
        || !Arrays.asList(eventDf.columns()).contains("eventID")) {
      log.debug("Material/event tables lack the columns needed for virtual material occurrences");
      return Optional.empty();
    }

    // evidenceForOccurrenceID is optional. When the column is absent, no material row can resolve
    // to a local occurrence, so all material rows are eligible for virtual-occurrence generation.
    Dataset<Row> eligible =
        materialColumns.contains(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)
            ? withoutLocallyResolvedEvidence(loader, materialDf)
            : materialDf;
    Dataset<Row> linked =
        UsagePolicyJoinBuilder.enrich(loader, eligible)
            .filter(functions.col(COLLECTION_EVENT_FK_COLUMN).isNotNull())
            .join(
                eventDf.select(
                    functions.col("event_pk").as("__collection_event_pk"),
                    functions.col("eventID")),
                functions
                    .col(COLLECTION_EVENT_FK_COLUMN)
                    .equalTo(functions.col("__collection_event_pk")),
                "inner");
    if (linked.isEmpty()) {
      log.debug(
          "No virtual material occurrences resolved for this dataset (material/event present but no unresolved-evidence rows with a resolvable collectionEvent_fk)");
      return Optional.empty();
    }

    List<Column> columns = new ArrayList<>();
    Column occurrenceId =
        materialColumns.contains(MATERIAL_ENTITY_ID_COLUMN)
            ? functions.coalesce(
                functions.col(MATERIAL_ENTITY_ID_COLUMN),
                functions.concat(
                    functions.lit(VIRTUAL_OCCURRENCE_ID_PREFIX),
                    functions.col(MATERIAL_ENTITY_PK_COLUMN)))
            : functions.concat(
                functions.lit(VIRTUAL_OCCURRENCE_ID_PREFIX),
                functions.col(MATERIAL_ENTITY_PK_COLUMN));
    columns.add(occurrenceId.as("occurrenceID"));
    columns.add(functions.col("eventID"));
    // Retained only while occurrence-side builders resolve material children; callers must drop it
    // before serialising the DwC occurrence term map.
    columns.add(functions.col(MATERIAL_ENTITY_PK_COLUMN));
    columns.add(functions.lit("MaterialSample").as("basisOfRecord"));
    columns.add(functions.lit("present").as("occurrenceStatus"));
    if (materialColumns.contains("materialSampleID")) {
      columns.add(functions.col("materialSampleID"));
    } else if (materialColumns.contains(MATERIAL_ENTITY_ID_COLUMN)) {
      columns.add(functions.col(MATERIAL_ENTITY_ID_COLUMN).as("materialSampleID"));
    }
    for (String column : materialDf.columns()) {
      if (!EXCLUDED_MATERIAL_COLUMNS.contains(column)
          && !MATERIAL_ENTITY_ID_COLUMN.equals(column)
          && !"materialSampleID".equals(column)) {
        columns.add(functions.col(column));
      }
    }
    return Optional.of(linked.select(columns.toArray(new Column[0])));
  }

  /**
   * Breakdown of what happened to every {@code material} row during conversion — for diagnostics
   * and the conversion report, not for production logic. The four leaf buckets are mutually
   * exclusive and sum to {@code total}:
   *
   * <ul>
   *   <li>{@code enrichedOntoRealOccurrence} — evidence resolves to a real local occurrence, and is
   *       its sole claimant; enriched that occurrence row via {@link #enrichOccurrences}.
   *   <li>{@code evidenceAmbiguous} — evidence resolves to a real local occurrence, but more than
   *       one material row claims the same one, so none of them were used (see {@link
   *       #singleMaterialPerOccurrence}).
   *   <li>{@code virtual} — evidence does <em>not</em> resolve to a real local occurrence (either
   *       null, or a weak reference to something outside this package — see {@link
   *       #virtualMaterialOccurrences}), but {@code collectionEvent_fk} resolved to a real event,
   *       so it became a virtual occurrence.
   *   <li>{@code unresolved} — evidence doesn't resolve locally, and {@code collectionEvent_fk} is
   *       either absent or doesn't resolve to any event in this package either. This row is
   *       silently dropped — it appears nowhere in the output. The bucket to watch when occurrences
   *       seem to go missing.
   * </ul>
   */
  public record MaterialFunnel(
      long total,
      long withEvidence,
      long enrichedOntoRealOccurrence,
      long evidenceAmbiguous,
      long withoutEvidence,
      long virtual,
      long unresolved) {}

  /**
   * Computes the {@link MaterialFunnel} breakdown for this package's {@code material} table. Reuses
   * {@link #singleMaterialPerOccurrence}, {@link #withEvidenceResolvedLocally}, and {@link
   * #virtualMaterialOccurrences} directly rather than re-implementing their filtering logic, so the
   * report can't drift from what conversion actually does. Returns {@link Optional#empty()} if
   * there is no {@code material} table or it lacks {@code evidenceForOccurrenceID}.
   */
  public static Optional<MaterialFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> materialDfOpt = loader.load(TABLE_MATERIAL);
    if (materialDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> materialDf = materialDfOpt.get();
    if (!Arrays.asList(materialDf.columns()).contains(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)) {
      return Optional.empty();
    }

    long total = materialDf.count();
    long withEvidence =
        materialDf.filter(functions.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN).isNotNull()).count();
    long withoutEvidence = total - withEvidence;

    // "Enriched"/"ambiguous" only make sense for evidence that actually resolves to a real local
    // occurrence — evidence pointing outside this package competes for nothing local, so multiple
    // materials citing the same external value isn't ambiguity, it's just several independent
    // virtual occurrences (see virtualMaterialOccurrences).
    long enriched =
        withEvidenceResolvedLocally(loader, singleMaterialPerOccurrence(materialDf)).count();
    long resolvedLocally = withEvidenceResolvedLocally(loader, materialDf).count();
    long evidenceAmbiguous = resolvedLocally - enriched;

    long virtualCount = virtualMaterialOccurrences(loader).map(Dataset::count).orElse(0L);
    long unresolved = total - enriched - evidenceAmbiguous - virtualCount;

    return Optional.of(
        new MaterialFunnel(
            total,
            withEvidence,
            enriched,
            evidenceAmbiguous,
            withoutEvidence,
            virtualCount,
            unresolved));
  }

  /** Returns virtual material ownership links, including the parent event identifier. */
  static Optional<Dataset<Row>> virtualMaterialOccurrenceLinks(TableLoader loader) {
    return virtualMaterialOccurrences(loader)
        .map(
            occurrences ->
                occurrences.select(
                    functions.col("occurrenceID"),
                    functions.col(MATERIAL_ENTITY_PK_COLUMN),
                    functions.col("eventID")));
  }

  /**
   * Left-joins the (already filtered to exactly-one-material-per-occurrence) material rows onto
   * occurrence via {@code occurrenceID -> evidenceForOccurrenceID} (both natural keys), adding only
   * columns occurrence doesn't already carry — same column-precedence policy as {@link
   * OrganismJoinBuilder#joinOrganism}.
   */
  private static Dataset<Row> join(Dataset<Row> occurrenceDf, Dataset<Row> materialDf) {
    Set<String> occurrenceCols = new HashSet<>(Arrays.asList(occurrenceDf.columns()));

    List<Column> selectCols = new ArrayList<>();
    for (String col : occurrenceDf.columns()) {
      selectCols.add(occurrenceDf.col(col));
    }
    for (String col : materialDf.columns()) {
      if (!occurrenceCols.contains(col) && !EXCLUDED_MATERIAL_COLUMNS.contains(col)) {
        selectCols.add(materialDf.col(col));
        log.debug("Adding material column '{}' to occurrence rows", col);
      }
    }

    Dataset<Row> joined =
        occurrenceDf
            .join(
                materialDf,
                occurrenceDf
                    .col("occurrenceID")
                    .equalTo(materialDf.col(EVIDENCE_FOR_OCCURRENCE_ID_COLUMN)),
                "left_outer")
            .select(selectCols.toArray(new Column[0]));

    log.info(
        "Material join complete: occurrence columns before={}, after={}",
        occurrenceDf.columns().length,
        joined.columns().length);

    return joined;
  }
}
