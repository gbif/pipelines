package org.gbif.pipelines.transforms.converters;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Emits a list of Edge records for each parent id in the list EventCoreRecord.getParentEventIds.
 */
@Data
@AllArgsConstructor(staticName = "of")
public abstract class ParentEventExpandTransform<T extends SpecificRecordBase & Record>
    implements Serializable {

  /** Location parent transform. */
  public static class LocationParentEventExpandTransform
      extends ParentEventExpandTransform<LocationRecord> {

    public LocationParentEventExpandTransform(
        TupleTag<LocationRecord> recordTupleTag,
        TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag);
    }
  }

  /** Temporal parent transform. */
  public static class TemporalParentEventExpandTransform
      extends ParentEventExpandTransform<TemporalRecord> {

    public TemporalParentEventExpandTransform(
        TupleTag<TemporalRecord> recordTupleTag,
        TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag);
    }
  }

  /** Taxon parent transform. */
  public static class TaxonParentEventExpandTransform
      extends ParentEventExpandTransform<TaxonRecord> {

    public TaxonParentEventExpandTransform(
        TupleTag<TaxonRecord> recordTupleTag, TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag);
    }
  }

  public static TaxonParentEventExpandTransform createTaxonTransform(
      TupleTag<TaxonRecord> recordTupleTag, TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
    return new TaxonParentEventExpandTransform(recordTupleTag, eventCoreRecordTupleTag);
  }

  public static LocationParentEventExpandTransform createLocationTransform(
      TupleTag<LocationRecord> recordTupleTag, TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
    return new LocationParentEventExpandTransform(recordTupleTag, eventCoreRecordTupleTag);
  }

  public static TemporalParentEventExpandTransform createTemporalTransform(
      TupleTag<TemporalRecord> recordTupleTag, TupleTag<EventCoreRecord> eventCoreRecordTupleTag) {
    return new TemporalParentEventExpandTransform(recordTupleTag, eventCoreRecordTupleTag);
  }

  /**
   * Graph edge to simplify the traversal of parent -> child relations.
   *
   * @param <E> content of the relation between fromId to toId
   */
  @Data
  @AllArgsConstructor(staticName = "of")
  public static class Edge<E> implements Serializable {

    private String fromId;
    private String toId;
    private E record;
  }

  private final TupleTag<T> recordTupleTag;

  private final TupleTag<EventCoreRecord> eventCoreRecordTupleTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, Edge<T>> converter() {
    return ParDo.of(
        new DoFn<KV<String, CoGbkResult>, Edge<T>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            EventCoreRecord eventCoreRecord = v.getOnly(eventCoreRecordTupleTag);
            T record = v.getOnly(recordTupleTag, null);
            if (eventCoreRecord.getParentsLineage() != null && record != null) {
              eventCoreRecord
                  .getParentsLineage()
                  .forEach(parent -> c.output(Edge.of(parent.getId(), record.getId(), record)));
            }
          }
        });
  }

  /** Creates a KV.of(Edge.fromId,T). */
  public MapElements<Edge<T>, KV<String, T>> asKv() {
    return MapElements.into(new TypeDescriptor<KV<String, T>>() {})
        .via((Edge<T> e) -> KV.of(e.fromId, e.record));
  }

  public PCollection<KV<String, T>> toSubEventsRecords(
      String recordName,
      PCollection<KV<String, T>> recordPCollection,
      PCollection<KV<String, EventCoreRecord>> eventCoreRecordPCollection) {
    return KeyedPCollectionTuple.of(eventCoreRecordTupleTag, eventCoreRecordPCollection)
        .and(recordTupleTag, recordPCollection)
        .apply("Grouping " + recordName + " and event records", CoGroupByKey.create())
        .apply("Collects " + recordName + " records in graph edges", converter())
        .apply("Converts the edge to parentId -> " + recordName + " record", asKv());
  }
}
