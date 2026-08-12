package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds Simple Multimedia extension Datasets ({@code Extension.MULTIMEDIA}, not Audubon Core) for
 * the event-core and occurrence-core paths.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>event-media / occurrence-media = media.media_pk (left outer), then resolved to
 *       eventID/occurrenceID
 *   <li>media enriched with usage-policy (license/rightsHolder) via {@link UsagePolicyJoinBuilder}
 *       before resolution
 *   <li>material-media (via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}) merged into
 *       the occurrence path alongside direct occurrence-media
 * </ul>
 *
 * <p>Event-core: occurrence/material media is promoted to the event's top-level extension — DwC-A
 * can't attach multimedia to a nested occurrence extension row (lossy; see mapping doc §7).
 *
 * <p><b>Deferred:</b> {@code media.creator}, {@code media-agent-role}. See mapping doc §4.5.
 */
@Slf4j
public class MediaExtensionBuilder {

  public static final String TABLE_MEDIA = "media";
  public static final String TABLE_EVENT_MEDIA = "event-media";
  public static final String TABLE_OCCURRENCE_MEDIA = "occurrence-media";
  public static final String TABLE_MATERIAL_MEDIA = "material-media";

  /** Extension.MULTIMEDIA.getRowType() — the real Simple Multimedia extension row type. */
  public static final String ROW_TYPE_MULTIMEDIA = Extension.MULTIMEDIA.getRowType();

  public static final String COL_MEDIA_EXT_JSON = "mediaExtJson";

  /** Maximum number of media rows promoted to a single event-core record. */
  private static final int MAX_MEDIA_PER_EVENT = 50;

  private MediaExtensionBuilder() {}

  /**
   * Three merged link paths: direct event-media, occurrence-media resolved through its event,
   * material-media resolved through its evidence occurrence (or virtual occurrence) then its event.
   */
  public static Optional<Dataset<Row>> buildEventMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> mediaDfOpt = loadEnrichedMedia(loader);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");

    if (mediaDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      log.debug(
          "Skipping event multimedia extension: media present={}, event present={}",
          mediaDfOpt.isPresent(),
          eventDfOpt.isPresent());
      return Optional.empty();
    }

    if (!Arrays.asList(eventDfOpt.get().columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping event multimedia extension");
      return Optional.empty();
    }

    Optional<Dataset<Row>> directEventMedia =
        buildDirectEventMediaRows(loader, mediaDfOpt.get(), eventDfOpt.get());
    Optional<Dataset<Row>> occurrenceMedia =
        buildOccurrenceMediaRowsForEvents(loader, mediaDfOpt.get(), eventDfOpt.get());
    Optional<Dataset<Row>> materialMedia =
        buildMaterialMediaRowsForEvents(loader, mediaDfOpt.get(), eventDfOpt.get());
    Optional<Dataset<Row>> combined =
        unionIfBothPresent(unionIfBothPresent(directEventMedia, occurrenceMedia), materialMedia);
    if (combined.isEmpty()) {
      log.debug("Skipping event multimedia extension: no direct or promoted media found");
      return Optional.empty();
    }

    Dataset<Row> withEventId = combined.get().dropDuplicates();
    Dataset<Row> limited = limitMediaPerEvent(withEventId);

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, limited, limited.columns(), "eventID", COL_MEDIA_EXT_JSON));
  }

  /**
   * Limits the lossy event-level media promotion to a deterministic subset.
   *
   * <p>Occurrence-level media is not limited here: this applies only to media promoted to the event
   * core because DwC-A cannot preserve its ownership by a nested occurrence extension.
   */
  private static Dataset<Row> limitMediaPerEvent(Dataset<Row> rows) {
    String[] mediaColumns =
        Arrays.stream(rows.columns())
            .filter(column -> !column.equals("eventID"))
            .toArray(String[]::new);

    org.apache.spark.sql.Column[] stableValueColumns =
        Arrays.stream(mediaColumns)
            .sorted()
            .map(functions::col)
            .toArray(org.apache.spark.sql.Column[]::new);

    WindowSpec eventWindow =
        Window.partitionBy("eventID")
            .orderBy(functions.sha2(functions.to_json(functions.struct(stableValueColumns)), 256));

    return rows.withColumn("__media_rank", functions.row_number().over(eventWindow))
        .filter(functions.col("__media_rank").leq(MAX_MEDIA_PER_EVENT))
        .drop("__media_rank");
  }

  private static Optional<Dataset<Row>> buildDirectEventMediaRows(
      TableLoader loader, Dataset<Row> mediaDf, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> eventMediaDfOpt = loader.load(TABLE_EVENT_MEDIA);
    if (eventMediaDfOpt.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        resolveParentId(
            joinMedia(eventMediaDfOpt.get(), mediaDf), "event_fk", eventDf, "event_pk", "eventID"));
  }

  private static Optional<Dataset<Row>> buildOccurrenceMediaRowsForEvents(
      TableLoader loader, Dataset<Row> mediaDf, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> occurrenceMediaRows =
        buildDirectOccurrenceMediaRows(loader, Optional.of(mediaDf));
    return occurrenceMediaRows.map(rows -> resolveOccurrenceRowsToEventId(rows, loader, eventDf));
  }

  private static Optional<Dataset<Row>> buildMaterialMediaRowsForEvents(
      TableLoader loader, Dataset<Row> mediaDf, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> materialMediaRows = buildMaterialMediaRows(loader, Optional.of(mediaDf));
    Optional<Dataset<Row>> evidenceRows =
        materialMediaRows.map(rows -> resolveOccurrenceRowsToEventId(rows, loader, eventDf));
    Optional<Dataset<Row>> virtualRows = buildVirtualMaterialMediaRowsForEvents(loader, mediaDf);
    return unionIfBothPresent(evidenceRows, virtualRows);
  }

  /**
   * Promotes media belonging to a virtual material occurrence directly to its collection event.
   * There is no real occurrence row to resolve through in this case.
   */
  private static Optional<Dataset<Row>> buildVirtualMaterialMediaRowsForEvents(
      TableLoader loader, Dataset<Row> mediaDf) {
    Optional<Dataset<Row>> materialMediaDfOpt = loader.load(TABLE_MATERIAL_MEDIA);
    Optional<Dataset<Row>> virtualLinksOpt =
        MaterialJoinBuilder.virtualMaterialOccurrenceLinks(loader);
    if (materialMediaDfOpt.isEmpty() || virtualLinksOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> mediaJoined = joinMedia(materialMediaDfOpt.get(), mediaDf);
    return Optional.of(
        resolveParentId(
            mediaJoined,
            "materialEntity_fk",
            virtualLinksOpt.get(),
            "materialEntity_pk",
            "eventID"));
  }

  /**
   * Resolves rows already carrying occurrenceID to an eventID, then removes occurrence ownership.
   */
  private static Dataset<Row> resolveOccurrenceRowsToEventId(
      Dataset<Row> rows, TableLoader loader, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");
    if (occurrenceDfOpt.isEmpty()) {
      // Keep an empty Dataset in the event-media shape: callers may union it with media resolved
      // through a virtual material occurrence, which is already keyed by eventID.
      return rows.limit(0)
          .drop("occurrenceID")
          .withColumn("eventID", functions.lit(null).cast("string"));
    }

    Dataset<Row> occurrenceToEvent =
        occurrenceDfOpt
            .get()
            .select(
                functions.col("occurrenceID").alias("__media_occurrence_id"),
                functions.col("event_fk"));
    Dataset<Row> withEventFk =
        rows.join(
                occurrenceToEvent,
                rows.col("occurrenceID").equalTo(occurrenceToEvent.col("__media_occurrence_id")),
                "left_outer")
            .drop(occurrenceToEvent.col("__media_occurrence_id"))
            .drop(rows.col("occurrenceID"));
    return resolveParentId(withEventFk, "event_fk", eventDf, "event_pk", "eventID");
  }

  /** Merges occurrence-media with material-media via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}. */
  public static Optional<Dataset<Row>> buildOccurrenceMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> mediaDfOpt = loadEnrichedMedia(loader);
    Optional<Dataset<Row>> fromOccurrenceMedia = buildDirectOccurrenceMediaRows(loader, mediaDfOpt);
    Optional<Dataset<Row>> fromMaterialMedia = buildMaterialMediaRows(loader, mediaDfOpt);

    Optional<Dataset<Row>> combined = unionIfBothPresent(fromOccurrenceMedia, fromMaterialMedia);
    if (combined.isEmpty()) {
      log.debug("Skipping occurrence-media extension: no direct or material-linked media found");
      return Optional.empty();
    }

    Dataset<Row> withOccurrenceId = combined.get();
    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark,
            withOccurrenceId,
            withOccurrenceId.columns(),
            "occurrenceID",
            COL_MEDIA_EXT_JSON));
  }

  /** Row-level (pre-aggregation) media rows from the direct {@code occurrence-media} link. */
  private static Optional<Dataset<Row>> buildDirectOccurrenceMediaRows(
      TableLoader loader, Optional<Dataset<Row>> mediaDfOpt) {
    Optional<Dataset<Row>> occMediaDfOpt = loader.load(TABLE_OCCURRENCE_MEDIA);
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");

    if (occMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty() || occurrenceDfOpt.isEmpty()) {
      log.debug(
          "Skipping direct occurrence-media rows: occurrence-media present={}, media present={}, occurrence present={}",
          occMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent(),
          occurrenceDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> mediaJoined = joinMedia(occMediaDfOpt.get(), mediaDfOpt.get());
    return Optional.of(
        resolveParentId(
            mediaJoined, "occurrence_fk", occurrenceDfOpt.get(), "occurrence_pk", "occurrenceID"));
  }

  /**
   * Row-level (pre-aggregation) media rows from {@code material-media}, resolved through {@link
   * MaterialJoinBuilder#singleMaterialOccurrenceLinks} down to the occurrence the material record
   * is exactly-one evidence for. Same column shape as {@link #buildDirectOccurrenceMediaRows} —
   * both ultimately come from the same {@code media} table via {@link #joinMedia} — so the two can
   * be unioned by name before aggregating.
   */
  private static Optional<Dataset<Row>> buildMaterialMediaRows(
      TableLoader loader, Optional<Dataset<Row>> mediaDfOpt) {
    Optional<Dataset<Row>> materialMediaDfOpt = loader.load(TABLE_MATERIAL_MEDIA);
    if (materialMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty()) {
      log.debug(
          "Skipping material-media rows: material-media present={}, media present={}",
          materialMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent());
      return Optional.empty();
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug("No single-material-per-occurrence links available; skipping material-media merge");
      return Optional.empty();
    }

    Dataset<Row> mediaJoined = joinMedia(materialMediaDfOpt.get(), mediaDfOpt.get());
    return Optional.of(
        resolveParentId(
            mediaJoined,
            "materialEntity_fk",
            materialLinksOpt.get(),
            "materialEntity_pk",
            "occurrenceID"));
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

  /**
   * Loads {@code media} and, if {@code usage-policy} is present, enriches it via {@link
   * UsagePolicyJoinBuilder#enrich} before either caller joins it to its own event/occurrence side.
   * Centralised here so both callers get the enrichment identically rather than each needing to
   * remember to apply it.
   */
  private static Optional<Dataset<Row>> loadEnrichedMedia(TableLoader loader) {
    return loader.load(TABLE_MEDIA).map(mediaDf -> UsagePolicyJoinBuilder.enrich(loader, mediaDf));
  }

  /**
   * Joins a media join-table ({@code event-media} or {@code occurrence-media}) to {@code media} on
   * {@code media_fk -> media.media_pk}. Neither join table carries {@code mediaID} — only the
   * surrogate FK — so {@code media_pk} is the only valid join key here.
   */
  private static Dataset<Row> joinMedia(Dataset<Row> joinTableDf, Dataset<Row> mediaDf) {
    return joinTableDf
        .join(mediaDf, joinTableDf.col("media_fk").equalTo(mediaDf.col("media_pk")), "inner")
        .drop(mediaDf.col("media_pk"))
        .drop(joinTableDf.col("media_fk"));
  }

  /**
   * Resolves a surrogate parent FK (e.g. {@code event_fk}) to the parent's natural id (e.g. {@code
   * eventID}) via a join, dropping both the parent's surrogate PK and the FK column itself so only
   * the resolved natural id remains — same shape as {@link AssertionExtensionBuilder}'s FK
   * resolution.
   *
   * <p>Rows where {@code parentIdColumn} comes back null (the FK didn't match any row in {@code
   * parentDf} at all — a genuinely dangling reference, or, for the material-media/material-
   * assertion merge, an entity that {@link MaterialJoinBuilder}'s exactly-one rule deliberately
   * excluded) are dropped here rather than allowed through: a left-outer join by itself would let
   * such a row survive with a null id, which {@link ExtensionAggregator#aggregateAsJsonByKey} would
   * then group under a null key — a real but meaningless output row, not nothing.
   */
  private static Dataset<Row> resolveParentId(
      Dataset<Row> df,
      String fkColumn,
      Dataset<Row> parentDf,
      String parentPkColumn,
      String parentIdColumn) {
    return df.join(
            parentDf.select(parentPkColumn, parentIdColumn),
            df.col(fkColumn).equalTo(parentDf.col(parentPkColumn)),
            "left_outer")
        .drop(parentDf.col(parentPkColumn))
        .drop(df.col(fkColumn))
        .filter(functions.col(parentIdColumn).isNotNull());
  }
}
