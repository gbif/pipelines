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
 * <p>DwC-DP expresses media attachment via two join tables: {@code event-media} (eventID → mediaID)
 * and {@code occurrence-media} (occurrenceID → mediaID). Each is joined to the {@code media} table
 * and aggregated by the relevant FK into a JSON column.
 *
 * <p>Both methods return {@link Optional#empty()} when either of their required source tables is
 * absent — media attachment is always optional in a DwC-DP package.
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
   * Returns a two-column Dataset {@code (eventID, mediaExtJson)}, or empty if either {@code
   * event-media} or {@code media} is absent.
   */
  public static Optional<Dataset<Row>> buildEventMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> eventMediaDfOpt = loader.load(TABLE_EVENT_MEDIA);
    Optional<Dataset<Row>> mediaDfOpt = loader.load(TABLE_MEDIA);

    if (eventMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty()) {
      log.debug(
          "Skipping event-media extension: event-media present={}, media present={}",
          eventMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> joined =
        eventMediaDfOpt
            .get()
            .join(
                mediaDfOpt.get(),
                eventMediaDfOpt.get().col("mediaID").equalTo(mediaDfOpt.get().col("mediaID")))
            .drop(mediaDfOpt.get().col("mediaID"));

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, joined, joined.columns(), "eventID", COL_MEDIA_EXT_JSON));
  }

  /**
   * Returns a two-column Dataset {@code (occurrenceID, mediaExtJson)}, or empty if either {@code
   * occurrence-media} or {@code media} is absent.
   */
  public static Optional<Dataset<Row>> buildOccurrenceMediaExtension(
      SparkSession spark, TableLoader loader) {

    Optional<Dataset<Row>> occMediaDfOpt = loader.load(TABLE_OCCURRENCE_MEDIA);
    Optional<Dataset<Row>> mediaDfOpt = loader.load(TABLE_MEDIA);

    if (occMediaDfOpt.isEmpty() || mediaDfOpt.isEmpty()) {
      log.debug(
          "Skipping occurrence-media extension: occurrence-media present={}, media present={}",
          occMediaDfOpt.isPresent(),
          mediaDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> joined =
        occMediaDfOpt
            .get()
            .join(
                mediaDfOpt.get(),
                occMediaDfOpt.get().col("mediaID").equalTo(mediaDfOpt.get().col("mediaID")))
            .drop(mediaDfOpt.get().col("mediaID"));

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, joined, joined.columns(), "occurrenceID", COL_MEDIA_EXT_JSON));
  }
}
