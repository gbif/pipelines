package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Occurrence extension Dataset for the event-core path.
 *
 * <p>When a DwC-DP dataset is ingested as event-core, occurrences become an extension rather than
 * the core. Organism fields are denormalized onto each occurrence row via {@link
 * OrganismJoinBuilder} before aggregation, so they are preserved on the extension rows.
 *
 * <p>{@code occurrence} only ever carries {@code event_fk} — a surrogate reference to {@code
 * event.event_pk} — it never carries a literal {@code eventID} column in the 1.0_DEV profile. This
 * builder resolves that FK to the natural {@code eventID} via the {@code event} table before
 * aggregating, the same pattern {@link AssertionExtensionBuilder} and {@link
 * HumboldtExtensionBuilder} already use. (Previously this checked for a literal {@code eventID}
 * column on {@code occurrence} and bailed when it wasn't there — which per the schema is always, so
 * the occurrence extension never attached to any event-core dataset.)
 *
 * <p>Returns a two-column Dataset {@code (eventID, occurrenceExtJson)} where the JSON is a
 * serialised list of term maps grouped by {@code eventID}. Returns {@link Optional#empty()} if the
 * occurrence table is absent, has no {@code event_fk} column, or the event table is absent.
 */
@Slf4j
public class OccurrenceExtensionBuilder {

  public static final String TABLE_OCCURRENCE = "occurrence";
  public static final String COL_OCCURRENCE_EXT_JSON = "occurrenceExtJson";

  private OccurrenceExtensionBuilder() {}

  /**
   * Builds the occurrence extension Dataset.
   *
   * @param spark active SparkSession (needed by {@link ExtensionAggregator})
   * @param loader table loader — returns {@link Optional#empty()} when a table is absent
   * @return two-column Dataset {@code (eventID, occurrenceExtJson)}, or empty if occurrence or
   *     event is absent, or occurrence has no {@code event_fk} column
   */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load(TABLE_OCCURRENCE);
    if (occurrenceDfOpt.isEmpty()) {
      log.debug("No occurrence table present; skipping occurrence extension");
      return Optional.empty();
    }

    Dataset<Row> occurrenceDf = occurrenceDfOpt.get();
    if (!Arrays.asList(occurrenceDf.columns()).contains("event_fk")) {
      log.warn("occurrence table has no event_fk column; skipping occurrence extension");
      return Optional.empty();
    }

    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()) {
      log.debug("No event table present; skipping occurrence extension");
      return Optional.empty();
    }

    Dataset<Row> enriched = OrganismJoinBuilder.enrichOccurrences(loader, occurrenceDf);

    Dataset<Row> withEventId =
        enriched
            .join(
                eventDfOpt.get().select("event_pk", "eventID"),
                enriched.col("event_fk").equalTo(eventDfOpt.get().col("event_pk")),
                "left_outer")
            .drop(eventDfOpt.get().col("event_pk"))
            .drop(enriched.col("event_fk"));

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, withEventId, withEventId.columns(), "eventID", COL_OCCURRENCE_EXT_JSON));
  }
}
