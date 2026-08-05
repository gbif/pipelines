package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves material → provenance links — the direct {@code material.provenance_fk} plus the
 * many-to-many {@code material-provenance} junction table, unioned and deduplicated — aggregates
 * the same four list-valued attribution fields {@link ProvenanceJoinBuilder} handles for {@code
 * event} (reusing {@link ProvenanceJoinBuilder#aggregateProvenanceFields} directly rather than
 * duplicating that struct-sort-transform-filter-join logic), and finally resolves the aggregated
 * result down to the occurrence the material record is exactly-one evidence for (via {@link
 * MaterialJoinBuilder#singleMaterialOccurrenceLinks}) before left-joining onto occurrence.
 *
 * <p>Simpler than the event case in one respect: {@code material-provenance.materialEntity_fk}
 * already equals {@code material.materialEntity_pk} directly — no extra hop through a freshly
 * reloaded parent table is needed to resolve the junction table's own FK, unlike {@code
 * event-provenance.event_fk}, which needs an {@code event} reload to get from the surrogate {@code
 * event_pk} to the natural {@code eventID}. Here, staying in {@code materialEntity_pk} surrogate
 * space until the final resolution step is exactly what's needed anyway, since {@link
 * MaterialJoinBuilder#singleMaterialOccurrenceLinks} itself expects a {@code materialEntity_pk} key
 * to resolve against.
 *
 * <p>No precedence conflict to resolve here, unlike most other material enrichments: {@code
 * occurrence} has no {@code fundingAttribution}/{@code fundingAttributionID}/{@code projectID}/
 * {@code projectTitle} fields of its own at all (only {@code event}/{@code provenance} do), so
 * there's nothing on the occurrence side these could ever collide with.
 */
@Slf4j
public class MaterialProvenanceJoinBuilder {

  static final String TABLE_MATERIAL_PROVENANCE = "material-provenance";
  static final String MATERIAL_ENTITY_PK_COLUMN = "materialEntity_pk";

  private MaterialProvenanceJoinBuilder() {}

  /**
   * Returns {@code occurrenceDf} enriched with pipe-delimited {@code fundingAttribution}, {@code
   * fundingAttributionID}, {@code projectID}, and {@code projectTitle} columns, aggregated across
   * every provenance record linked to the occurrence's own material (direct FK and/or junction
   * table on {@code material}, deduplicated, sorted by {@code provenanceID}, resolved through the
   * same exactly-one-material rule {@link MaterialJoinBuilder} applies elsewhere). Returns {@code
   * occurrenceDf} unchanged if {@code provenance} or a usable material/occurrence link is absent.
   */
  public static Dataset<Row> enrichOccurrences(TableLoader loader, Dataset<Row> occurrenceDf) {
    Optional<Dataset<Row>> provenanceDfOpt = loader.load(ProvenanceJoinBuilder.TABLE_PROVENANCE);
    if (provenanceDfOpt.isEmpty()) {
      log.debug("No provenance table present; skipping material provenance attribution join");
      return occurrenceDf;
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug(
          "No single-material-per-occurrence links available; skipping material provenance "
              + "attribution join");
      return occurrenceDf;
    }

    Optional<Dataset<Row>> materialDfOpt = loader.load(MaterialJoinBuilder.TABLE_MATERIAL);
    if (materialDfOpt.isEmpty()) {
      // Shouldn't happen given materialLinksOpt is present, but defensive regardless.
      log.debug(
          "material table unexpectedly absent; skipping material provenance attribution join");
      return occurrenceDf;
    }

    Dataset<Row> links = collectMaterialProvenanceLinks(loader, materialDfOpt.get());
    if (links == null) {
      log.debug(
          "No provenance links found on material (no direct provenance_fk column and no "
              + "material-provenance junction table); skipping material provenance attribution "
              + "join");
      return occurrenceDf;
    }

    Dataset<Row> provenanceDf = provenanceDfOpt.get();
    // left_outer, not inner — same reasoning as ProvenanceJoinBuilder.enrichEvents: never let a
    // dangling provenance_fk collapse the aggregation input to zero rows, which can interact
    // badly with Spark's empty-relation optimizer once fed through a groupBy/agg chain.
    Dataset<Row> joined =
        links
            .join(
                provenanceDf,
                links
                    .col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN)
                    .equalTo(provenanceDf.col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN)),
                "left_outer")
            .drop(provenanceDf.col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN));

    Dataset<Row> aggregatedByMaterial =
        ProvenanceJoinBuilder.aggregateProvenanceFields(joined, MATERIAL_ENTITY_PK_COLUMN);

    Dataset<Row> materialLinks = materialLinksOpt.get();
    Dataset<Row> aggregatedByOccurrence =
        aggregatedByMaterial
            .join(
                materialLinks,
                aggregatedByMaterial
                    .col(MATERIAL_ENTITY_PK_COLUMN)
                    .equalTo(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN)),
                "left_outer")
            .drop(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN))
            .drop(aggregatedByMaterial.col(MATERIAL_ENTITY_PK_COLUMN))
            // A material record excluded by the exactly-one rule (or otherwise not present in
            // materialLinks) resolves to a null occurrenceID here — dropped rather than allowed
            // to survive as a null-keyed row, same policy applied throughout this session.
            .filter(functions.col("occurrenceID").isNotNull());

    Dataset<Row> joinedOccurrence =
        occurrenceDf.join(
            aggregatedByOccurrence,
            occurrenceDf.col("occurrenceID").equalTo(aggregatedByOccurrence.col("occurrenceID")),
            "left_outer");
    List<Column> columns = new ArrayList<>();
    for (String column : occurrenceDf.columns()) {
      columns.add(occurrenceDf.col(column));
    }
    for (String column : aggregatedByOccurrence.columns()) {
      if (!"occurrenceID".equals(column)
          && !MATERIAL_ENTITY_PK_COLUMN.equals(column)
          && !Arrays.asList(occurrenceDf.columns()).contains(column)) {
        columns.add(aggregatedByOccurrence.col(column));
      }
    }
    return joinedOccurrence.select(columns.toArray(new Column[0]));
  }

  /**
   * Builds a two-column {@code (materialEntity_pk, provenance_pk)} Dataset — one row per distinct
   * material/provenance link — from the direct FK, the junction table, or both (deduplicated via
   * {@code union().distinct()}). Returns {@code null} if neither source of links is present at all.
   *
   * <p>Unlike {@link ProvenanceJoinBuilder}'s equivalent, the junction table needs no extra hop
   * through a freshly reloaded parent table — {@code material-provenance.materialEntity_fk} already
   * equals {@code material.materialEntity_pk} directly.
   */
  private static Dataset<Row> collectMaterialProvenanceLinks(
      TableLoader loader, Dataset<Row> materialDf) {
    boolean hasDirectFk =
        Arrays.asList(materialDf.columns()).contains(ProvenanceJoinBuilder.PROVENANCE_FK_COLUMN);
    Optional<Dataset<Row>> junctionDfOpt = loader.load(TABLE_MATERIAL_PROVENANCE);

    Dataset<Row> direct =
        hasDirectFk
            ? materialDf
                .select(
                    functions.col(MATERIAL_ENTITY_PK_COLUMN),
                    functions
                        .col(ProvenanceJoinBuilder.PROVENANCE_FK_COLUMN)
                        .as(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN))
                .filter(functions.col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN).isNotNull())
            : null;

    Dataset<Row> junction =
        junctionDfOpt
            .map(
                junctionDf ->
                    junctionDf.select(
                        functions.col("materialEntity_fk").as(MATERIAL_ENTITY_PK_COLUMN),
                        functions
                            .col(ProvenanceJoinBuilder.PROVENANCE_FK_COLUMN)
                            .as(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN)))
            .orElse(null);

    if (direct == null && junction == null) {
      return null;
    }
    if (direct == null) {
      return junction.distinct();
    }
    if (junction == null) {
      return direct.distinct();
    }
    return direct.unionByName(junction).distinct();
  }

  /**
   * Computes a {@link JoinFunnel} breakdown of occurrence funding/project attribution via single
   * material evidence, mirroring {@link #enrichOccurrences}'s decision logic (reuses {@link
   * #collectMaterialProvenanceLinks} directly). Same three-bucket shape as {@link
   * ProvenanceJoinBuilder#computeFunnel}, but based on the {@code materialEntity_pk} → {@code
   * occurrenceID} resolution from {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks} rather
   * than events directly. Buckets are mutually exclusive and sum to the base count.
   *
   * @return empty if {@code provenance}, the {@code material} table, or any unambiguous
   *     single-material occurrence link is absent — same no-op cases {@link #enrichOccurrences}
   *     treats as absent
   */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> provenanceDfOpt = loader.load(ProvenanceJoinBuilder.TABLE_PROVENANCE);
    if (provenanceDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      return Optional.empty();
    }
    Optional<Dataset<Row>> materialDfOpt = loader.load(MaterialJoinBuilder.TABLE_MATERIAL);
    if (materialDfOpt.isEmpty()) {
      return Optional.empty();
    }

    String label =
        "MaterialProvenanceJoinBuilder (occurrence funding/project attribution via single "
            + "material)";
    Dataset<Row> materialLinks = materialLinksOpt.get();
    long base = materialLinks.count();
    if (base == 0L) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(new JoinFunnel.Bucket("unambiguous single-material links (base)", 0L))));
    }

    Dataset<Row> links = collectMaterialProvenanceLinks(loader, materialDfOpt.get());
    if (links == null) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket("unambiguous single-material links (base)", base),
                  new JoinFunnel.Bucket("no material-provenance link", base))));
    }

    Dataset<Row> linkedMaterials = links.select(MATERIAL_ENTITY_PK_COLUMN).distinct();
    long linkedOccurrences =
        linkedMaterials
            .join(
                materialLinks,
                linkedMaterials
                    .col(MATERIAL_ENTITY_PK_COLUMN)
                    .equalTo(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN)),
                "left_semi")
            .count();
    long noLink = base - linkedOccurrences;

    Dataset<Row> provenanceDf = provenanceDfOpt.get();
    Dataset<Row> resolvedMaterials =
        links
            .join(
                provenanceDf,
                links
                    .col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN)
                    .equalTo(provenanceDf.col(ProvenanceJoinBuilder.PROVENANCE_PK_COLUMN)),
                "left_semi")
            .select(MATERIAL_ENTITY_PK_COLUMN)
            .distinct();
    long occurrencesWithAttribution =
        resolvedMaterials
            .join(
                materialLinks,
                resolvedMaterials
                    .col(MATERIAL_ENTITY_PK_COLUMN)
                    .equalTo(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN)),
                "left_semi")
            .count();
    long linkedButAllDangling = linkedOccurrences - occurrencesWithAttribution;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("unambiguous single-material links (base)", base),
                new JoinFunnel.Bucket("no material-provenance link", noLink),
                new JoinFunnel.Bucket("linked, attribution merged", occurrencesWithAttribution),
                new JoinFunnel.Bucket(
                    "linked, but all links dangling (no attribution)", linkedButAllDangling))));
  }
}
