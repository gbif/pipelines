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
 * <p>Returns a two-column Dataset {@code (eventID, occurrenceExtJson)} where the JSON is a
 * serialised list of term maps grouped by {@code eventID}. Returns {@link Optional#empty()} if the
 * occurrence table is absent or carries no {@code eventID} column.
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
   * @return two-column Dataset {@code (eventID, occurrenceExtJson)}, or empty if occurrence table
   *     is absent or has no {@code eventID} column
   */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load(TABLE_OCCURRENCE);
    if (occurrenceDfOpt.isEmpty()) {
      log.debug("No occurrence table present; skipping occurrence extension");
      return Optional.empty();
    }

    Dataset<Row> occurrenceDf = occurrenceDfOpt.get();
    if (!Arrays.asList(occurrenceDf.columns()).contains("eventID")) {
      log.warn("occurrence table has no eventID column; skipping occurrence extension");
      return Optional.empty();
    }

    Dataset<Row> enriched = OrganismJoinBuilder.enrichOccurrences(loader, occurrenceDf);

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, enriched, enriched.columns(), "eventID", COL_OCCURRENCE_EXT_JSON));
  }
}
