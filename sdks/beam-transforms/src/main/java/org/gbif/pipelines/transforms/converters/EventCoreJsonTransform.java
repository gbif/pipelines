package org.gbif.pipelines.transforms.converters;

import java.io.Serializable;

import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import lombok.Builder;
import lombok.NonNull;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

/**
 * Beam level transformation for the ES output json. The transformation consumes objects, which
 * classes were generated from avro schema files and converts into json string object
 *
 * <p>Example:
 *
 * <p>
 *
 * <pre>{@code
 * final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
 * final TupleTag<EventCoreRecord> ecrTag = new TupleTag<EventCoreRecord>() {};
 *
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, EventCoreRecord>> eventCoreRecordCollection = ...
 *
 * SingleOutput<KV<String, CoGbkResult>, String> eventCoreJsonDoFn =
 *     EventCoreJsonTransform.create(erTag, ecrTag)
 *         .converter();
 *
 * PCollection<String> jsonCollection =
 *     KeyedPCollectionTuple
 *         // Core
 *         .of(ecrTag, eventCoreRecordCollection)
 *         // Raw
 *         .and(erTag, verbatimCollection)
 *         // Apply
 *         .apply("Grouping objects", CoGroupByKey.create())
 *         .apply("Merging to json", eventCoreJsonDoFn);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class EventCoreJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806170L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(EventCoreJsonTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            EventCoreRecord ecr = v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());

            String json = OccurrenceJsonConverter.toStringJson(ecr, er);

            c.output(json);

            counter.inc();
          }
        };

    return ParDo.of(fn);
  }
}
