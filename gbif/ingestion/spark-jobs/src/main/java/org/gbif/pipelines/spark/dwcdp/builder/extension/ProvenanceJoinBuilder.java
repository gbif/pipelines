package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves event → provenance links — the direct {@code event.provenance_fk} plus the many-to-many
 * {@code event-provenance} junction table, unioned and deduplicated — and aggregates the
 * list-valued attribution fields onto each event as pipe-delimited strings, sorted by {@code
 * provenanceID} for deterministic output regardless of Spark's internal shuffle order.
 *
 * <p>Only four provenance fields are handled: {@code fundingAttribution}, {@code
 * fundingAttributionID}, {@code projectID}, {@code projectTitle}. These are confirmed list-valued
 * downstream — {@code CoreInterpreter.interpretFundingAttribution}/{@code
 * interpretFundingAttributionID}/{@code interpretProjectID}/{@code interpretProjectTitle} all call
 * {@code extractListValue} (via {@code interpretStringList}), the same pipe-delimited convention
 * used elsewhere in DwC (e.g. {@code recordedBy}) — so aggregating across every linked provenance
 * record and pipe-joining is a faithful fit for the field, not a lossy compromise.
 *
 * <p>{@code provenance.references} is deliberately left untouched: {@code
 * CoreInterpreter.interpretReferences} reads that term as a single value ({@code extractValue}, not
 * {@code extractListValue}), and {@code event.eventReferences} already populates it directly —
 * pipe-joining provenance's copy in would corrupt a single-valued field and create an unresolved
 * precedence question against the existing source. {@code datasetID} is likewise left alone, since
 * it's already sourced directly from {@code event.datasetID}. The remaining provenance fields
 * ({@code source}, {@code creator}, {@code providerLiteral}, {@code metadataCreatorLiteral}, {@code
 * metadataProviderLiteral}, {@code furtherInformationURL}, {@code feedbackURL}, {@code
 * bibliographicCitation}, and their {@code *ID} counterparts) have no confirmed downstream
 * interpreter target and are not handled here — same situation as {@code creator} on media.
 */
@Slf4j
public class ProvenanceJoinBuilder {

  public static final String TABLE_PROVENANCE = "provenance";
  public static final String TABLE_EVENT_PROVENANCE = "event-provenance";

  static final String PROVENANCE_PK_COLUMN = "provenance_pk";
  static final String PROVENANCE_ID_COLUMN = "provenanceID";
  static final String PROVENANCE_FK_COLUMN = "provenance_fk";

  private static final List<String> AGGREGATED_FIELDS =
      List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle");

  private static final String DELIMITER = "|";

  private ProvenanceJoinBuilder() {}

  /**
   * Returns {@code eventDf} enriched with pipe-delimited {@code fundingAttribution}, {@code
   * fundingAttributionID}, {@code projectID}, and {@code projectTitle} columns, aggregated across
   * every provenance record linked to each event (direct FK and/or junction table, deduplicated,
   * sorted by {@code provenanceID}). Returns {@code eventDf} unchanged if the {@code provenance}
   * table is absent, or if {@code eventDf} has no {@code eventID} column.
   */
  public static Dataset<Row> enrichEvents(TableLoader loader, Dataset<Row> eventDf) {
    Optional<Dataset<Row>> provenanceDfOpt = loader.load(TABLE_PROVENANCE);
    if (provenanceDfOpt.isEmpty()) {
      log.debug("No provenance table present; skipping provenance attribution join");
      return eventDf;
    }

    if (!Arrays.asList(eventDf.columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping provenance attribution join");
      return eventDf;
    }

    Dataset<Row> links = collectLinks(loader, eventDf);
    Dataset<Row> withDirectFkDropped =
        Arrays.asList(eventDf.columns()).contains(PROVENANCE_FK_COLUMN)
            ? eventDf.drop(PROVENANCE_FK_COLUMN)
            : eventDf;

    if (links == null) {
      log.debug(
          "No provenance links found (no direct provenance_fk column and no event-provenance "
              + "junction table); skipping provenance attribution join");
      return withDirectFkDropped;
    }

    Dataset<Row> provenanceDf = provenanceDfOpt.get();
    // left_outer, not inner: an event whose only linked provenance record is a dangling FK
    // (points at a provenance_pk that doesn't exist) must still produce exactly one row here —
    // with nulls for the provenance-side columns — so it survives into a non-empty `aggregated`
    // below. An inner join would make `joined` (and then `aggregated`) genuinely zero-row for
    // such an event, and a zero-row DataFrame produced by a groupBy/agg chain can be optimized
    // by Spark's Catalyst empty-relation propagation in ways that don't behave like a naive
    // "left-outer-join against nothing preserves the left side" mental model — better to never
    // construct that zero-row intermediate at all than to rely on a later join surviving it.
    Dataset<Row> joined =
        links
            .join(
                provenanceDf,
                links.col(PROVENANCE_PK_COLUMN).equalTo(provenanceDf.col(PROVENANCE_PK_COLUMN)),
                "left_outer")
            .drop(provenanceDf.col(PROVENANCE_PK_COLUMN));

    Dataset<Row> aggregated = aggregateProvenanceFields(joined, "eventID");

    return withDirectFkDropped
        .join(
            aggregated,
            withDirectFkDropped.col("eventID").equalTo(aggregated.col("eventID")),
            "left_outer")
        .drop(aggregated.col("eventID"));
  }

  /**
   * Builds a two-column {@code (eventID, provenance_pk)} Dataset — one row per distinct
   * event/provenance link — from the direct FK, the junction table, or both (deduplicated via
   * {@code union().distinct()}, so an event linked to the same provenance record both ways isn't
   * double-counted). Returns {@code null} if neither source of links is present at all.
   *
   * <p>The junction table resolves its own {@code event_fk} against a fresh {@code event_pk}/
   * {@code eventID} lookup reloaded directly from the loader, rather than relying on columns still
   * being present on the {@code eventDf} passed in — {@code EventCoreBuilder} may already have
   * dropped {@code event_pk} by the time this runs, depending on call order, so this makes the
   * resolution self-contained regardless of pipeline ordering.
   */
  private static Dataset<Row> collectLinks(TableLoader loader, Dataset<Row> eventDf) {
    boolean hasDirectFk = Arrays.asList(eventDf.columns()).contains(PROVENANCE_FK_COLUMN);
    Optional<Dataset<Row>> junctionDfOpt = loader.load(TABLE_EVENT_PROVENANCE);

    Dataset<Row> direct =
        hasDirectFk
            ? eventDf
                .select(col("eventID"), col(PROVENANCE_FK_COLUMN).as(PROVENANCE_PK_COLUMN))
                .filter(col(PROVENANCE_PK_COLUMN).isNotNull())
            : null;

    Dataset<Row> junction = null;
    if (junctionDfOpt.isPresent()) {
      Optional<Dataset<Row>> eventPkLookupOpt = loader.load("event");
      if (eventPkLookupOpt.isPresent()) {
        Dataset<Row> eventPkLookup = eventPkLookupOpt.get().select("event_pk", "eventID");
        Dataset<Row> junctionDf = junctionDfOpt.get();
        junction =
            junctionDf
                .join(
                    eventPkLookup,
                    junctionDf.col("event_fk").equalTo(eventPkLookup.col("event_pk")),
                    "inner")
                .select(
                    eventPkLookup.col("eventID"),
                    junctionDf.col(PROVENANCE_FK_COLUMN).as(PROVENANCE_PK_COLUMN));
      } else {
        log.warn(
            "event-provenance present but event table could not be reloaded; skipping "
                + "junction-table provenance links");
      }
    }

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
   * Groups {@code joined} (links already joined to the {@code provenance} table) by {@code
   * keyColumn}, and for each of {@link #AGGREGATED_FIELDS} that's actually present on {@code
   * joined}, collects the values sorted by {@code provenanceID}, drops nulls, and pipe-joins them.
   * None of these four fields are marked required in the DwC-DP schema, so a given dataset's {@code
   * provenance} table may genuinely arrive without one of them altogether — not just null-valued,
   * but the column itself absent after schema inference — so each is checked for presence first,
   * same defensive pattern {@link GeologicalContextJoinBuilder} and {@link UsagePolicyJoinBuilder}
   * already apply to their own optional columns.
   *
   * <p>A row whose linked provenance records all have a null value for a given field ends up with
   * an empty array after the null drop — {@code array_join} on that yields {@code ""}, not {@code
   * null}, so each result is explicitly nulled back out when empty, matching this codebase's
   * convention of an absent field rather than an empty-string one.
   *
   * <p>Package-private rather than private, and parameterized by {@code keyColumn} rather than
   * hardcoding {@code eventID}: {@link MaterialProvenanceJoinBuilder} reuses this identical
   * aggregation logic (grouping by {@code materialEntity_pk} instead) rather than duplicating the
   * struct-sort-transform-filter-join expression a second time.
   */
  static Dataset<Row> aggregateProvenanceFields(Dataset<Row> joined, String keyColumn) {
    List<String> joinedColumns = Arrays.asList(joined.columns());

    // provenanceID has no `required: true` constraint in the DwC-DP profile (only provenance_pk
    // does), so a dataset's provenance table can legitimately arrive without it — not merely
    // null-valued, absent from the schema entirely, same situation as eventID/occurrenceID
    // elsewhere in this codebase. Falling back to provenance_pk (guaranteed present here via
    // `links`, for both this method's callers — see class javadoc) only affects the deterministic
    // *order* of the pipe-delimited output, never which values end up in it.
    String sortKeyColumn =
        joinedColumns.contains(PROVENANCE_ID_COLUMN) ? PROVENANCE_ID_COLUMN : PROVENANCE_PK_COLUMN;

    List<Column> aggs = new ArrayList<>();
    for (String field : AGGREGATED_FIELDS) {
      if (!joinedColumns.contains(field)) {
        log.debug("provenance table has no '{}' column; skipping its aggregation", field);
        continue;
      }
      Column sortedStructs = array_sort(collect_list(struct(col(sortKeyColumn), col(field))));
      Column sortedValues = transform(sortedStructs, x -> x.getField(field));
      Column nonNullValues = filter(sortedValues, Column::isNotNull);
      Column joinedString = array_join(nonNullValues, DELIMITER);
      aggs.add(when(length(joinedString).equalTo(0), lit(null)).otherwise(joinedString).as(field));
    }

    if (aggs.isEmpty()) {
      // None of the four fields are present on this dataset's provenance table at all — still
      // return distinct keys so the caller's left-outer join has something valid to join against
      // (a harmless no-op enrichment rather than an empty aggregation).
      return joined.select(col(keyColumn)).distinct();
    }

    return joined
        .groupBy(col(keyColumn))
        .agg(aggs.get(0), aggs.subList(1, aggs.size()).toArray(new Column[0]));
  }

  /**
   * Computes a {@link JoinFunnel} breakdown of event → provenance attribution linking, mirroring
   * {@link #enrichEvents}'s decision logic (reusing {@link #collectLinks} directly so the two can't
   * drift apart). Buckets are mutually exclusive and sum to the total event count:
   *
   * <ul>
   *   <li><b>no provenance link</b> — no direct {@code provenance_fk} and no {@code
   *       event-provenance} junction row for this event
   *   <li><b>linked, attribution merged</b> — at least one link resolves to a real {@code
   *       provenance} row, so {@link #enrichEvents} merges at least some attribution data in
   *   <li><b>linked, but all links dangling (no attribution)</b> — every link this event has points
   *       at a {@code provenance_pk} that doesn't exist; {@link #enrichEvents}'s left-outer join
   *       still produces a row for it, but with nulls for every provenance-side field
   * </ul>
   *
   * <p>Reloads {@code event} fresh via {@code loader} rather than taking an already-enriched
   * Dataset — note this means the candidate/total count reflects the raw {@code event} table as
   * declared in {@code datapackage.json}, not any {@code eventID} null-fallback applied later in
   * {@code EventCoreBuilder} before this join actually runs in production.
   *
   * @return empty if {@code provenance} is absent, {@code event} is absent, or {@code event} has no
   *     {@code eventID} column — same cases {@link #enrichEvents} treats as a no-op
   */
  public static Optional<JoinFunnel> computeFunnel(TableLoader loader) {
    Optional<Dataset<Row>> provenanceDfOpt = loader.load(TABLE_PROVENANCE);
    if (provenanceDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> eventDf = eventDfOpt.get();
    if (!Arrays.asList(eventDf.columns()).contains("eventID")) {
      return Optional.empty();
    }

    String label = "ProvenanceJoinBuilder (event funding/project attribution)";
    long totalEvents = eventDf.count();

    Dataset<Row> links = collectLinks(loader, eventDf);
    if (links == null) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  new JoinFunnel.Bucket("events (total)", totalEvents),
                  new JoinFunnel.Bucket(
                      "no provenance link (no direct FK, no junction)", totalEvents))));
    }

    long linkedEvents = links.select("eventID").distinct().count();
    long unlinkedEvents = totalEvents - linkedEvents;

    Dataset<Row> provenanceDf = provenanceDfOpt.get();
    long eventsWithAttribution =
        links
            .join(
                provenanceDf,
                links.col(PROVENANCE_PK_COLUMN).equalTo(provenanceDf.col(PROVENANCE_PK_COLUMN)),
                "left_semi")
            .select("eventID")
            .distinct()
            .count();
    long eventsLinkedButAllDangling = linkedEvents - eventsWithAttribution;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                new JoinFunnel.Bucket("events (total)", totalEvents),
                new JoinFunnel.Bucket("no provenance link", unlinkedEvents),
                new JoinFunnel.Bucket("linked, attribution merged", eventsWithAttribution),
                new JoinFunnel.Bucket(
                    "linked, but all links dangling (no attribution)",
                    eventsLinkedButAllDangling))));
  }
}
