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
import org.gbif.pipelines.io.avro.Record;

/**
 * Emits a list of Edge records for each parent id in the list EventCoreRecord.getParentEventIds.
 */
@Data
@AllArgsConstructor(staticName = "of")
public class ParentEventExpandTransform<T extends SpecificRecordBase & Record>
    implements Serializable {

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
            if (eventCoreRecord.getParentEventIds() != null && record != null) {
              eventCoreRecord
                  .getParentEventIds()
                  .forEach(parentId -> c.output(Edge.of(parentId, record.getId(), record)));
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