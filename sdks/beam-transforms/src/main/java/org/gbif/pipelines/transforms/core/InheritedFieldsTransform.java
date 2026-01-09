package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

@Slf4j
@Builder
public class InheritedFieldsTransform implements Serializable {

  public static final TupleTag<LocationInheritedRecord> LIR_TAG =
      new TupleTag<LocationInheritedRecord>() {};

  public static final TupleTag<TemporalInheritedRecord> TIR_TAG =
      new TupleTag<TemporalInheritedRecord>() {};

  public static final TupleTag<EventInheritedRecord> EIR_TAG =
      new TupleTag<EventInheritedRecord>() {};

  public ParDo.SingleOutput<EventCoreRecord, KV<String, Edge<EventCoreRecord>>>
      childToParentEdgeConverter() {
    DoFn<EventCoreRecord, KV<String, Edge<EventCoreRecord>>> fn =
        new DoFn<EventCoreRecord, KV<String, Edge<EventCoreRecord>>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            EventCoreRecord eventCoreRecord = c.element();
            c.output(
                KV.of(
                    eventCoreRecord.getId(),
                    Edge.of(eventCoreRecord.getId(), eventCoreRecord.getId(), eventCoreRecord)));

            if (eventCoreRecord.getParentsLineage() != null
                && !eventCoreRecord.getParentsLineage().isEmpty()) {
              // workaround for https://github.com/gbif/pipelines/issues/1231
              for (Object rawObj : eventCoreRecord.getParentsLineage()) {
                Parent parent = null;

                try {
                  if (rawObj instanceof Parent) {
                    parent = (Parent) rawObj;
                  } else if (rawObj instanceof org.apache.avro.generic.GenericRecord) {
                    org.apache.avro.generic.GenericRecord gr =
                        (org.apache.avro.generic.GenericRecord) rawObj;
                    parent = new Parent();
                    if (gr.get("id") != null) {
                      parent.setId(gr.get("id").toString());
                    }
                    if (gr.get("eventType") != null) {
                      parent.setEventType(gr.get("eventType").toString());
                    }
                    if (gr.get("verbatimEventType") != null) {
                      parent.setVerbatimEventType(gr.get("verbatimEventType").toString());
                    }
                    if (gr.get("order") != null) {
                      parent.setOrder(Integer.parseInt(gr.get("order").toString()));
                    }
                  }

                  if (parent != null && parent.getId() != null) {
                    c.output(
                        KV.of(
                            parent.getId(),
                            Edge.of(parent.getId(), eventCoreRecord.getId(), eventCoreRecord)));
                  }

                } catch (Exception e) {
                  log.warn(
                      "Failed inheriting parent events data for event {}",
                      eventCoreRecord.getId(),
                      e);
                }
              }
            }
          }
        };
    return ParDo.of(fn);
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, KV<String, EventCoreRecord>>
      childToParentConverter(EventCoreTransform eventCoreTransform) {
    DoFn<KV<String, CoGbkResult>, KV<String, EventCoreRecord>> fn =
        new DoFn<KV<String, CoGbkResult>, KV<String, EventCoreRecord>>() {
          @DoFn.ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult result = c.element().getValue();
            EventCoreRecord eventCoreRecord = result.getOnly(eventCoreTransform.getTag());
            Iterable<Edge<EventCoreRecord>> children =
                result.getAll(eventCoreTransform.getEdgeTag());
            children.forEach(child -> c.output(KV.of(child.getToId(), eventCoreRecord)));
          }
        };
    return ParDo.of(fn);
  }
}
