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
 * Enriches occurrence rows with institution/collection/specimen fields from {@code material}
 * (feeds GrSciColl matching and the institutionCode+collectionCode+catalogNumber GBIF ID triplet).
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>material.evidenceForOccurrenceID = occurrence.occurrenceID (left outer, natural-key, weak
 *       FK), gated to exactly-one-material-per-occurrence
 *   <li>material enriched with usage-policy (license/rightsHolder) via {@link
 *       UsagePolicyJoinBuilder#enrich} before the above
 * </ul>
 *
 * <p>Occurrence's own value always wins on overlapping fields. {@code
 * materialEntity_pk}/{@code collectionEvent_fk}/{@code derivationEvent_fk}/{@code
 * provenance_fk}/{@code usagePolicy_fk} are excluded from what gets copied over.
 *
 * <p>{@code material-media}/{@code material-assertion}/{@code material-identifier}/{@code
 * material-provenance}/{@code material-protocol}/{@code material-geological-context} are handled
 * by other builders reusing {@link #singleMaterialOccurrenceLinks} — see mapping doc §4.4 for the
 * full list and current gaps ({@code collectionEvent_fk}/{@code derivationEvent_fk} excluded
 * unconditionally; virtual-occurrence synthesis currently paused, §3.5).
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

  /**
   * PAUSED — single choke point for every virtual-occurrence path in this class ({@link
   * #virtualMaterialOccurrenceLinks}, {@link #singleMaterialOccurrenceLinks}, {@link
   * #computeFunnel}) and external callers. Flipping to {@code true} is sufficient to re-enable;
   * no other change needed. While paused, affected materials fall into {@link
   * MaterialFunnel#unresolved}. See mapping doc §3.5.
   */
  private static final boolean VIRTUAL_MATERIAL_OCCURRENCES_ENABLED = false;

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
   * Rows whose {@code evidenceForOccurrenceID} does not resolve to a real local occurrence
   * (includes null and out-of-package references — a weak FK, not required to resolve locally).
   * Used by {@link #virtualMaterialOccurrences} and (via its complement below) {@link
   * #singleMaterialOccurrenceLinks}, so the two stay mutually exclusive.
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

  /** Complement of {@link #withoutLocallyResolvedEvidence}. */
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

  /** {@code occurrenceDf} unchanged if material is absent, or the natural keys are missing on either side. */
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
   * Filters to non-null {@code evidenceForOccurrenceID}, keeps only groups of exactly one. Reused
   * by {@link MediaExtensionBuilder}/{@link AssertionExtensionBuilder} via {@link
   * #singleMaterialOccurrenceLinks}.
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
   * {@code (occurrenceID, materialEntity_pk)} — one row per occurrence with exactly one material
   * citing it as evidence, unioned with virtual-occurrence links. Reused by other builders to
   * resolve a material-side FK down to its owning occurrence.
   *
   * <p>Returns empty (not a zero-row Dataset) when nothing resolves at all — must look identical to
   * "material absent" to every caller, or a left-outer join against a genuinely zero-row Dataset
   * can silently produce a null-keyed row downstream instead of contributing nothing.
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
   * Materials with no locally-resolving evidence link but a resolvable {@code collectionEvent_fk}
   * → synthesised occurrence under that event. See mapping doc §3.5 (paused).
   */
  public static Optional<Dataset<Row>> virtualMaterialOccurrences(TableLoader loader) {
    if (!VIRTUAL_MATERIAL_OCCURRENCES_ENABLED) {
      log.debug("Virtual material occurrence synthesis is currently paused; skipping");
      return Optional.empty();
    }

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
   * Buckets sum to {@code total}: {@code enrichedOntoRealOccurrence} (sole claimant of a real local
   * occurrence), {@code evidenceAmbiguous} (multiple materials claim the same occurrence — none
   * used), {@code virtual} (no local occurrence, but collectionEvent_fk resolved), {@code
   * unresolved} (dropped — the bucket to watch when occurrences go missing).
   */
  public record MaterialFunnel(
      long total,
      long withEvidence,
      long enrichedOntoRealOccurrence,
      long evidenceAmbiguous,
      long withoutEvidence,
      long virtual,
      long unresolved) {}

  /** Reuses production filtering logic directly, so the report can't drift from actual conversion. */
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

  /** Pure join transform, occurrence value wins on overlap. */
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
