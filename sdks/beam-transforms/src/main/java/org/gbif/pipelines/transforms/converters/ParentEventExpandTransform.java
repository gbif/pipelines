package org.gbif.pipelines.transforms.converters;

import java.io.Serializable;
import lombok.Data;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.AvroCoder;
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
import org.gbif.pipelines.common.beam.coders.EdgeCoder;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Emits a list of Edge records for each parent id in the list EventCoreRecord.getParentEventIds.
 */
@Data
public class ParentEventExpandTransform<T extends SpecificRecordBase & Record>
    implements Serializable {

  /** Location parent transform. */
  public static class LocationParentEventExpandTransform
      extends ParentEventExpandTransform<LocationRecord> {

    public LocationParentEventExpandTransform(
        TupleTag<LocationRecord> recordTupleTag,
        TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
        TupleTag<Edge<LocationRecord>> edgeTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag, LocationRecord.class);
    }
  }

  /** Temporal parent transform. */
  public static class TemporalParentEventExpandTransform
      extends ParentEventExpandTransform<TemporalRecord> {

    public TemporalParentEventExpandTransform(
        TupleTag<TemporalRecord> recordTupleTag,
        TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
        TupleTag<Edge<TemporalRecord>> edgeTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag, TemporalRecord.class);
    }
  }

  /** Taxon parent transform. */
  public static class TaxonParentEventExpandTransform
      extends ParentEventExpandTransform<TaxonRecord> {

    public TaxonParentEventExpandTransform(
        TupleTag<TaxonRecord> recordTupleTag,
        TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
        TupleTag<Edge<TaxonRecord>> edgeTupleTag) {
      super(recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag, TaxonRecord.class);
    }
  }

  public static TaxonParentEventExpandTransform createTaxonTransform(
      TupleTag<TaxonRecord> recordTupleTag,
      TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
      TupleTag<Edge<TaxonRecord>> edgeTupleTag) {
    return new TaxonParentEventExpandTransform(
        recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag);
  }

  public static LocationParentEventExpandTransform createLocationTransform(
      TupleTag<LocationRecord> recordTupleTag,
      TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
      TupleTag<Edge<LocationRecord>> edgeTupleTag) {
    return new LocationParentEventExpandTransform(
        recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag);
  }

  public static TemporalParentEventExpandTransform createTemporalTransform(
      TupleTag<TemporalRecord> recordTupleTag,
      TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
      TupleTag<Edge<TemporalRecord>> edgeTupleTag) {
    return new TemporalParentEventExpandTransform(
        recordTupleTag, eventCoreRecordTupleTag, edgeTupleTag);
  }

  private final TupleTag<T> recordTupleTag;

  private final TupleTag<Edge<T>> edgeTupleTag;

  private final TupleTag<EventCoreRecord> eventCoreRecordTupleTag;

  private final EdgeCoder<T> edgeCoder;

  public ParentEventExpandTransform(
      TupleTag<T> recordTupleTag,
      TupleTag<EventCoreRecord> eventCoreRecordTupleTag,
      TupleTag<Edge<T>> edgeTupleTag,
      Class<T> recordClass) {
    this.recordTupleTag = recordTupleTag;
    this.eventCoreRecordTupleTag = eventCoreRecordTupleTag;
    this.edgeCoder = EdgeCoder.of(AvroCoder.of(recordClass));
    this.edgeTupleTag = edgeTupleTag;
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, Edge<T>> converter() {
    return ParDo.of(
        new DoFn<KV<String, CoGbkResult>, Edge<T>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            EventCoreRecord eventCoreRecord = v.getOnly(eventCoreRecordTupleTag);
            T r = v.getOnly(recordTupleTag, null);
            if (eventCoreRecord.getParentsLineage() != null && r != null) {
              eventCoreRecord
                  .getParentsLineage()
                  .forEach(p -> c.output(Edge.of(p.getId(), r.getId(), r)));
            }
          }
        });
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, Edge<T>> edgeConverter() {
    return ParDo.of(
        new DoFn<KV<String, CoGbkResult>, Edge<T>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            Iterable<Edge<T>> edges = v.getAll(edgeTupleTag);
            edges.forEach(
                edge -> {
                  T r = v.getOnly(recordTupleTag, null);
                  if (r != null) {
                    c.output(Edge.of(edge.getFromId(), edge.getToId(), r));
                  }
                });
          }
        });
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, Edge<T>>>
      parentToChildEdgeConverter() {
    return ParDo.of(
        new DoFn<KV<String, CoGbkResult>, KV<String, Edge<T>>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            EventCoreRecord eventCoreRecord = v.getOnly(eventCoreRecordTupleTag);
            T r = v.getOnly(recordTupleTag, null);
            if (r != null) {
              c.output(KV.of(eventCoreRecord.getId(), Edge.of(r.getId(), r.getId(), r)));
              if (eventCoreRecord.getParentsLineage() != null) {
                eventCoreRecord
                    .getParentsLineage()
                    .forEach(p -> c.output(KV.of(p.getId(), Edge.of(r.getId(), p.getId(), r))));
              }
            }
          }
        });
  }

  /** Creates a KV.of(Edge.fromId,T). */
  public MapElements<Edge<T>, KV<String, T>> asKv() {
    return MapElements.into(new TypeDescriptor<KV<String, T>>() {})
        .via((Edge<T> e) -> KV.of(e.getFromId(), e.getRecord()));
  }

  public PCollection<KV<String, T>> toSubEventsRecords(
      String recordName,
      PCollection<KV<String, T>> recordPCollection,
      PCollection<KV<String, EventCoreRecord>> eventCoreRecordPCollection) {
    return KeyedPCollectionTuple.of(eventCoreRecordTupleTag, eventCoreRecordPCollection)
        .and(recordTupleTag, recordPCollection)
        .apply("Grouping " + recordName + " and event records", CoGroupByKey.create())
        .apply("Collects " + recordName + " records in graph edges", converter())
        .setCoder(edgeCoder)
        .apply("Converts the edge to parentId -> " + recordName + " record", asKv())
        .setCoder(edgeCoder.getKvRecordCoder());
  }

  public PCollection<KV<String, T>> toSubEventsRecordsFromLeaf(
      String recordName,
      PCollection<KV<String, T>> recordPCollection,
      PCollection<KV<String, EventCoreRecord>> eventCoreRecordPCollection) {
    PCollection<KV<String, Edge<T>>> edges =
        KeyedPCollectionTuple.of(eventCoreRecordTupleTag, eventCoreRecordPCollection)
            .and(recordTupleTag, recordPCollection)
            .apply(
                "Grouping " + recordName + " and event records from parent", CoGroupByKey.create())
            .apply(
                "Collects " + recordName + " records in graph edges from parent",
                parentToChildEdgeConverter())
            .setCoder(edgeCoder.getKvEdgeCoder());

    return KeyedPCollectionTuple.of(edgeTupleTag, edges)
        .and(recordTupleTag, recordPCollection)
        .apply("Grouping parents " + recordName + " records", CoGroupByKey.create())
        .apply("Collects parents " + recordName + " records in graph edges", edgeConverter())
        .setCoder(edgeCoder)
        .apply("Converts the edge to parentId -> " + recordName + " (child) record", asKv())
        .setCoder(edgeCoder.getKvRecordCoder());
  }
}
