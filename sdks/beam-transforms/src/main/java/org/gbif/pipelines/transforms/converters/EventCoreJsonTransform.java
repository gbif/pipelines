package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENT_AVRO_TO_JSON_COUNT;

import java.io.Serializable;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.EventCoreJsonConverter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

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
 * final TupleTag<IdentifierRecord> irTag = new TupleTag<IdentifierRecord>() {};
 * final TupleTag<EventCoreRecord> ecrTag = new TupleTag<EventCoreRecord>() {};
 *
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, EventCoreRecord>> eventCoreRecordCollection = ...
 * PCollection<KV<String, IdentifierRecord>> identifierRecordCollection = ...
 *
 * SingleOutput<KV<String, CoGbkResult>, String> eventCoreJsonDoFn =
 * EventCoreJsonTransform.builder()
 *    .extendedRecordTag(verbatimTransform.getTag())
 *    .identifierRecordTag(identifierTransform.getTag())
 *    .eventCoreRecordTag(eventCoreTransform.getTag())
 *    .build()
 *    .converter();
 *
 * PCollection<String> jsonCollection =
 *    KeyedPCollectionTuple
 *    // Core
 *    .of(eventCoreTransform.getTag(), eventCoreCollection)
 *    // Internal
 *    .and(identifierTransform.getTag(), identifierCollection)
 *    // Raw
 *    .and(verbatimTransform.getTag(), verbatimCollection)
 *    // Apply
 *    .apply("Grouping objects", CoGroupByKey.create())
 *    .apply("Merging to json", eventCoreJsonDoFn);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class EventCoreJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final TupleTag<IdentifierRecord> identifierRecordTag;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(EventCoreJsonTransform.class, EVENT_AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            EventCoreRecord ecr =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());
            IdentifierRecord ir =
                v.getOnly(identifierRecordTag, IdentifierRecord.newBuilder().setId(k).build());

            String json = EventCoreJsonConverter.toStringJson(ecr, ir, er);

            c.output(json);

            counter.inc();
          }
        };

    return ParDo.of(fn);
  }
}
