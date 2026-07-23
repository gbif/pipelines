package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds Multimedia extension Datasets for the event-core and occurrence-core paths.
 *
 * <p>DwC-DP expresses media attachment via two join tables: {@code event-media} ({@code event_fk},
 * {@code media_fk}) and {@code occurrence-media} ({@code occurrence_fk}, {@code media_fk}). Neither
 * join table carries a natural {@code eventID}/{@code occurrenceID} column, and neither carries
 * {@code mediaID} either — only the surrogate {@code _fk} columns, per the 1.0_DEV profile.
 * (Previously both methods here joined to {@code media} on {@code "mediaID" = "mediaID"}, which
 * doesn't exist on either join table at all — a hard join failure, not a silent one — and then
 * grouped by {@code eventID}/{@code occurrenceID} directly, which also never exists on either join
 * table.)
 *
 * <p>Both methods now: (1) join to {@code media} on {@code media_fk -> media.media_pk}, then (2)
 * resolve the parent-side FK to its natural id via the {@code event}/{@code occurrence} table — the
 * same resolve-then-aggregate pattern {@link AssertionExtensionBuilder} and {@link
 * HumboldtExtensionBuilder} already use — before aggregating.
 *
 * <p>Both methods return {@link Optional#empty()} when any required source table is absent — media
 * attachment is always optional in a DwC-DP package.
 */
@Slf4j
public class MediaExtensionBuilder {

  public static final String TABLE_MEDIA = "media";
  public static final String TABLE_EVENT_MEDIA = "event-media";
  public static final String TABLE_OCCURRENCE_MEDIA = "occurrence-media";

  public static final String ROW_TYPE_MULTIMEDIA = "http://rs.tdwg.org/ac/terms/Multimedia";

  public static final String COL_MEDIA_EXT_JSON = "mediaExtJson";

  private MediaExtensionBuilder() {}

  /**
   * Returns a two-column Dataset {@code (eventID, mediaExtJson)}, or empty if {@code event-media},
   * {@code media}, or {@code event} is absent.
   */
  public static Optional<Dataset<Row>> buildEventMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> eventMediaDfOpt = loader.load(TABLE_EVENT_MEDIA);
    Optional<Dataset<Row>> mediaDfOpt = loader.load(TABLE_MEDIA);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");

    if (eventMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      log.debug(
          "Skipping event-media extension: event-media present={}, media present={}, event present={}",
          eventMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent(),
          eventDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> mediaJoined = joinMedia(eventMediaDfOpt.get(), mediaDfOpt.get());
    Dataset<Row> withEventId =
        resolveParentId(mediaJoined, "event_fk", eventDfOpt.get(), "event_pk", "eventID");

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, withEventId, withEventId.columns(), "eventID", COL_MEDIA_EXT_JSON));
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, mediaExtJson)}, or empty if {@code
   * occurrence-media}, {@code media}, or {@code occurrence} is absent.
   */
  public static Optional<Dataset<Row>> buildOccurrenceMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> occMediaDfOpt = loader.load(TABLE_OCCURRENCE_MEDIA);
    Optional<Dataset<Row>> mediaDfOpt = loader.load(TABLE_MEDIA);
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load("occurrence");

    if (occMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty() || occurrenceDfOpt.isEmpty()) {
      log.debug(
          "Skipping occurrence-media extension: occurrence-media present={}, media present={}, occurrence present={}",
          occMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent(),
          occurrenceDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> mediaJoined = joinMedia(occMediaDfOpt.get(), mediaDfOpt.get());
    Dataset<Row> withOccurrenceId =
        resolveParentId(
            mediaJoined, "occurrence_fk", occurrenceDfOpt.get(), "occurrence_pk", "occurrenceID");

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark,
            withOccurrenceId,
            withOccurrenceId.columns(),
            "occurrenceID",
            COL_MEDIA_EXT_JSON));
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
        .drop(df.col(fkColumn));
  }
}
