package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.util.DatasetJoins;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Occurrence extension Dataset for the event-core path (occurrences nested under event
 * rather than being core themselves).
 *
 * <p><b>Joins:</b> mirrors {@link org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder} —
 * {@link OrganismJoinBuilder}, {@link IdentificationJoinBuilder}, {@link MaterialJoinBuilder} (+
 * satellites), {@link AgentJoinBuilder}; folds in occurrence-media/assertion/identification-history/
 * identifier as nested JSON before aggregation.
 *
 * <p>occurrence.event_fk = event.event_pk (left outer) resolved to natural eventID before
 * aggregating.
 *
 * <p>Returns a two-column Dataset {@code (eventID, occurrenceExtJson)}. Materials without an
 * evidence occurrence are represented as virtual occurrences when {@code collectionEvent_fk}
 * resolves — currently paused, see mapping doc §3.5.
 */
@Slf4j
public class OccurrenceExtensionBuilder {

  public static final String TABLE_OCCURRENCE = "occurrence";
  public static final String COL_OCCURRENCE_EXT_JSON = "occurrenceExtJson";

  private static final String OCCURRENCE_ID_COLUMN = "occurrenceID";
  private static final String OCCURRENCE_PK_COLUMN = "occurrence_pk";

  private OccurrenceExtensionBuilder() {}

  /** Empty if event is absent, or no real/virtual occurrence resolves to one. */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()) {
      log.debug("No event table present; skipping occurrence extension");
      return Optional.empty();
    }

    if (!Arrays.asList(eventDfOpt.get().columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping occurrence extension");
      return Optional.empty();
    }

    Optional<Dataset<Row>> realOccurrences = buildRealOccurrences(loader, eventDfOpt.get());
    Optional<Dataset<Row>> virtualOccurrences =
        MaterialJoinBuilder.virtualMaterialOccurrences(loader);
    if (realOccurrences.isEmpty() && virtualOccurrences.isEmpty()) {
      log.debug("No real or virtual occurrence rows available for event-core extension");
      return Optional.empty();
    }

    Dataset<Row> occurrences =
        realOccurrences
            .map(
                real ->
                    virtualOccurrences.map(virtual -> real.unionByName(virtual, true)).orElse(real))
            .orElseGet(virtualOccurrences::get);

    // Virtual rows already carry their material fields. The three material child joins use the
    // shared material-to-occurrence links, which include those virtual rows.
    occurrences = MaterialGeologicalContextJoinBuilder.enrichOccurrences(loader, occurrences);
    occurrences = MaterialProvenanceJoinBuilder.enrichOccurrences(loader, occurrences);
    occurrences = MaterialProtocolJoinBuilder.enrichOccurrences(loader, occurrences);

    // Attach the occurrence's own media/assertion extensions before resolving event_fk and
    // aggregating, so they ride along as nested JSON on each occurrence's term map — the same
    // way OccurrenceCoreBuilder attaches them when occurrence is core, rather than being
    // silently dropped when occurrence is nested under an event core instead.
    Optional<Dataset<Row>> occMediaExtDf =
        MediaExtensionBuilder.buildOccurrenceMediaExtension(spark, loader);
    Optional<Dataset<Row>> occAssertionExtDf =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(spark, loader);
    Optional<Dataset<Row>> occIdentificationExtDf =
        IdentificationExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> occIdentifierExtDf = IdentifierExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> occDnaExtDf = NucleotideExtensionBuilder.buildOccurrence(spark, loader);

    Dataset<Row> withOwnExtensions =
        DatasetJoins.leftJoinIfPresent(occurrences, occMediaExtDf, OCCURRENCE_ID_COLUMN);
    withOwnExtensions =
        DatasetJoins.leftJoinIfPresent(withOwnExtensions, occAssertionExtDf, OCCURRENCE_ID_COLUMN);
    withOwnExtensions =
        DatasetJoins.leftJoinIfPresent(
            withOwnExtensions, occIdentificationExtDf, OCCURRENCE_ID_COLUMN);
    withOwnExtensions =
        DatasetJoins.leftJoinIfPresent(withOwnExtensions, occIdentifierExtDf, OCCURRENCE_ID_COLUMN);
    withOwnExtensions =
        DatasetJoins.leftJoinIfPresent(withOwnExtensions, occDnaExtDf, OCCURRENCE_ID_COLUMN);
    // occurrence_pk was needed by IdentificationJoinBuilder above (directly, on this local
    // Dataset) and by MediaExtensionBuilder/AssertionExtensionBuilder/
    // IdentificationExtensionBuilder/IdentifierExtensionBuilder's own independent occurrence_fk
    // resolution (each reloads "occurrence" fresh from the loader for that) — it has no DwC term
    // of its own and must not survive into this occurrence's nested term map inside
    // occurrenceExtJson.
    Dataset<Row> withEventId =
        withOwnExtensions.drop(OCCURRENCE_PK_COLUMN).drop("materialEntity_pk");

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, withEventId, withEventId.columns(), "eventID", COL_OCCURRENCE_EXT_JSON));
  }

  private static Optional<Dataset<Row>> buildRealOccurrences(
      TableLoader loader, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> occurrenceDfOpt = loader.load(TABLE_OCCURRENCE);
    if (occurrenceDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> occurrenceDf = occurrenceDfOpt.get();
    if (!Arrays.asList(occurrenceDf.columns()).contains("event_fk")) {
      log.warn("occurrence table has no event_fk column; skipping real occurrence extension");
      return Optional.empty();
    }

    Dataset<Row> enriched = OrganismJoinBuilder.enrichOccurrences(loader, occurrenceDf);
    enriched = IdentificationJoinBuilder.enrichOccurrences(loader, enriched);
    enriched = MaterialJoinBuilder.enrichOccurrences(loader, enriched);
    enriched =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            loader, enriched, "recordedByID", "recordedBy");
    enriched =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            loader, enriched, "identifiedByID", "identifiedBy");
    return Optional.of(
        enriched
            .join(
                eventDf.select("event_pk", "eventID"),
                enriched.col("event_fk").equalTo(eventDf.col("event_pk")),
                "inner")
            .drop(eventDf.col("event_pk"))
            .drop(enriched.col("event_fk")));
  }
}
