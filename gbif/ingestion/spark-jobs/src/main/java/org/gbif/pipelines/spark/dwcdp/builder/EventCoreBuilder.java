package org.gbif.pipelines.spark.dwcdp.builder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.GeologicalContextJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OccurrenceExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProtocolJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProvenanceJoinBuilder;
import org.gbif.pipelines.spark.util.DatasetJoins;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds a distributed Dataset of event-core {@link ExtendedRecord}s from DwC-DP Parquet files.
 *
 * <p>Pipeline:
 *
 * <ol>
 *   <li>Load the required {@code event} table — throws if absent (routing error).
 *   <li>Resolve {@code parentEvent_fk} (a surrogate self-reference to {@code event.event_pk}) to
 *       the parent's natural {@code eventID}, so {@code
 *       org.gbif.pipelines.core.interpreters.core.CoreInterpreter#interpretParentEventID} has
 *       something to read. See {@link #resolveParentEventId}.
 *   <li>Enrich with {@code geological-context} via {@link GeologicalContextJoinBuilder} — skipped
 *       if absent or {@code event} has no {@code geologicalContextID} column.
 *   <li>Resolve {@code eventProtocol_fk} → new {@code samplingProtocol} column, and coalesce {@code
 *       georeferenceProtocol_fk} into the existing {@code georeferenceProtocol} text field (only
 *       where it's null) via {@link ProtocolJoinBuilder} — previously both FKs leaked as raw
 *       surrogate values under their own column names.
 *   <li>Enrich with {@code fundingAttribution}/{@code fundingAttributionID}/{@code projectID}/
 *       {@code projectTitle} via {@link ProvenanceJoinBuilder} — aggregated, pipe-delimited, across
 *       every linked {@code provenance} record (direct FK and/or {@code event-provenance} junction
 *       table), sorted by {@code provenanceID} for deterministic output.
 *   <li>Build the Occurrence extension via {@link OccurrenceExtensionBuilder} — skipped if the
 *       occurrence table is absent or has no {@code eventID} column. Organism fields, and the
 *       occurrence's own {@code occurrence-media}/{@code occurrence-assertion} rows, are already
 *       denormalized/nested onto occurrence rows inside that builder.
 *   <li>Build the Multimedia extension via {@link MediaExtensionBuilder} — skipped if either {@code
 *       event-media} or {@code media} is absent.
 *   <li>Build the eMoF extension via {@link AssertionExtensionBuilder} — skipped if {@code
 *       event-assertion} is absent.
 *   <li>Build the Identifier extension via {@link IdentifierExtensionBuilder} — skipped if {@code
 *       event-identifier} is absent.
 *   <li>Build the Humboldt extension via {@link HumboldtExtensionBuilder} — skipped if {@code
 *       survey} is absent.
 *   <li>Map each joined row to an {@link ExtendedRecord} with {@code coreRowType = dwc:Event}.
 * </ol>
 */
@Slf4j
public class EventCoreBuilder {

  private static final String CORE_ROW_TYPE = DwcTerm.Event.qualifiedName();
  private static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  private static final String EVENT_PK_COLUMN = "event_pk";
  private static final String PARENT_EVENT_FK_COLUMN = "parentEvent_fk";
  private static final String PARENT_EVENT_ID_COLUMN = "parentEventID";
  private static final String PARENT_JOIN_ALIAS_COLUMN = "__parent_event_pk";

  private EventCoreBuilder() {}

  /**
   * Builds the event-core ExtendedRecord Dataset.
   *
   * @param spark active SparkSession
   * @param loader table loader — {@link Optional#empty()} signals a table is absent from the
   *     package
   * @throws IllegalStateException if the event table is absent (caller routing error)
   */
  public static Dataset<ExtendedRecord> build(SparkSession spark, TableLoader loader) {

    Dataset<Row> eventDf =
        loader
            .load("event")
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "event table missing — orchestrator should not have routed here"));

    eventDf = resolveParentEventId(eventDf);
    eventDf = GeologicalContextJoinBuilder.enrichEvents(loader, eventDf);
    eventDf =
        ProtocolJoinBuilder.resolveProtocolFk(
            loader, eventDf, "eventProtocol_fk", "samplingProtocol");
    eventDf =
        ProtocolJoinBuilder.mergeJunctionProtocolsInto(
            eventDf,
            ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
                loader, "event-protocol", "event_fk", "event", "event_pk", "eventID"),
            "eventID",
            "eventID",
            "samplingProtocol");
    eventDf =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            loader, eventDf, "georeferenceProtocol_fk", "georeferenceProtocol");
    eventDf = ProvenanceJoinBuilder.enrichEvents(loader, eventDf);

    Optional<Dataset<Row>> occurrenceExtDf = OccurrenceExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> mediaExtDf =
        MediaExtensionBuilder.buildEventMediaExtension(spark, loader);
    Optional<Dataset<Row>> assertionExtDf =
        AssertionExtensionBuilder.buildEventAssertionExtension(spark, loader);
    Optional<Dataset<Row>> identifierExtDf = IdentifierExtensionBuilder.buildEvent(spark, loader);
    Optional<Dataset<Row>> humboldtExtDf = HumboldtExtensionBuilder.build(spark, loader);

    Dataset<Row> joined = eventDf;
    joined = DatasetJoins.leftJoinIfPresent(joined, occurrenceExtDf, "eventID");
    joined = DatasetJoins.leftJoinIfPresent(joined, mediaExtDf, "eventID");
    joined = DatasetJoins.leftJoinIfPresent(joined, assertionExtDf, "eventID");
    joined = DatasetJoins.leftJoinIfPresent(joined, identifierExtDf, "eventID");
    joined = DatasetJoins.leftJoinIfPresent(joined, humboldtExtDf, "eventID");

    final String[] eventColumns = eventDf.columns();
    final boolean hasOccExt = occurrenceExtDf.isPresent();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();
    final boolean hasIdentifierExt = identifierExtDf.isPresent();
    final boolean hasHumboldtExt = humboldtExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row ->
                    toExtendedRecord(
                        row,
                        eventColumns,
                        hasOccExt,
                        hasMediaExt,
                        hasAssertionExt,
                        hasIdentifierExt,
                        hasHumboldtExt),
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  /**
   * Resolves {@code parentEvent_fk} (a surrogate self-reference to {@code event.event_pk}) to the
   * parent event's natural {@code eventID}, replacing it with a column literally named {@code
   * parentEventID} — which {@code TermFactory} already resolves directly to {@code
   * DwcTerm.parentEventID}, so no {@link DwcDpTermMappings} entry is needed. The original {@code
   * parentEvent_fk} and the bare {@code event_pk} surrogate (needed only for this join, never a
   * real DwC term) are dropped afterwards — in both the resolved and the no-parent-column branches,
   * since {@code event_pk} has no business leaking into {@code coreTerms} either way.
   *
   * <p>Does not filter or otherwise treat self-referencing rows (a row whose {@code parentEvent_fk}
   * happens to point at its own {@code event_pk}) specially — {@code DwcTerm.parentEventID} still
   * gets resolved to that row's own {@code eventID}, and it's {@code
   * CoreInterpreter.interpretParentEventID}'s job downstream to detect that and flag {@code
   * PARENT_EVENT_INFINITE_LINEAGE}. Resolving it correctly here is what lets that detection fire at
   * all.
   *
   * <p>Skips the join itself (but still drops {@code event_pk} if present) when {@code eventDf} has
   * no {@code parentEvent_fk} column (most events have no parent-child hierarchy) or no {@code
   * event_pk} column to resolve against.
   */
  private static Dataset<Row> resolveParentEventId(Dataset<Row> eventDf) {
    List<String> columns = Arrays.asList(eventDf.columns());
    if (!columns.contains(PARENT_EVENT_FK_COLUMN) || !columns.contains(EVENT_PK_COLUMN)) {
      return eventDf.drop(EVENT_PK_COLUMN);
    }

    Dataset<Row> parentIds =
        eventDf.select(
            eventDf.col(EVENT_PK_COLUMN).as(PARENT_JOIN_ALIAS_COLUMN),
            eventDf.col("eventID").as(PARENT_EVENT_ID_COLUMN));

    return eventDf
        .join(
            parentIds,
            eventDf.col(PARENT_EVENT_FK_COLUMN).equalTo(parentIds.col(PARENT_JOIN_ALIAS_COLUMN)),
            "left_outer")
        .drop(PARENT_JOIN_ALIAS_COLUMN)
        .drop(eventDf.col(PARENT_EVENT_FK_COLUMN))
        .drop(EVENT_PK_COLUMN);
  }

  private static ExtendedRecord toExtendedRecord(
      Row row,
      String[] eventColumns,
      boolean hasOccExt,
      boolean hasMediaExt,
      boolean hasAssertionExt,
      boolean hasIdentifierExt,
      boolean hasHumboldtExt)
      throws IOException {

    String eventId = RowTermMapper.safeGet(row, "eventID");
    if (eventId == null || eventId.isEmpty()) {
      return null;
    }

    Map<String, String> coreTerms = RowTermMapper.toTermMap(row, eventColumns);
    Map<String, List<Map<String, String>>> extensions = new HashMap<>();

    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasOccExt,
        OccurrenceExtensionBuilder.COL_OCCURRENCE_EXT_JSON,
        ROW_TYPE_OCCURRENCE);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasMediaExt,
        MediaExtensionBuilder.COL_MEDIA_EXT_JSON,
        MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasAssertionExt,
        AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON,
        AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasIdentifierExt,
        IdentifierExtensionBuilder.COL_IDENTIFIER_EXT_JSON,
        IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasHumboldtExt,
        HumboldtExtensionBuilder.COL_HUMBOLDT_EXT_JSON,
        HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT);

    return ExtendedRecord.newBuilder()
        .setId(eventId)
        .setCoreId(null)
        .setCoreRowType(CORE_ROW_TYPE)
        .setCoreTerms(coreTerms)
        .setExtensions(extensions)
        .build();
  }
}
