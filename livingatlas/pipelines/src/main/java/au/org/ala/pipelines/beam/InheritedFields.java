package au.org.ala.pipelines.beam;

import lombok.Builder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.gbif.pipelines.common.beam.coders.AvroKvCoder;
import org.gbif.pipelines.core.pojo.Edge;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.transforms.converters.ParentEventExpandTransform;
import org.gbif.pipelines.transforms.core.*;

/**
 * Copy of the inner class used in @{@link
 * org.gbif.pipelines.ingest.pipelines.EventToEsIndexPipeline} Moved here to avoid dependency on
 * ingest-gbif-beam
 */
@Builder
final class InheritedFields {

  private final InheritedFieldsTransform inheritedFieldsTransform;
  private final TemporalTransform temporalTransform;
  private final EventCoreTransform eventCoreTransform;
  private final LocationTransform locationTransform;
  private final PCollection<KV<String, TemporalRecord>> temporalCollection;
  private final PCollection<KV<String, LocationRecord>> locationCollection;
  private final PCollection<KV<String, EventCoreRecord>> eventCoreCollection;

  PCollection<KV<String, LocationInheritedRecord>> inheritLocationFields() {
    PCollection<KV<String, LocationRecord>> locationRecordsOfSubEvents =
        ParentEventExpandTransform.createLocationTransform(
                locationTransform.getTag(),
                eventCoreTransform.getTag(),
                locationTransform.getEdgeTag())
            .toSubEventsRecordsFromLeaf("Location", locationCollection, eventCoreCollection);

    return PCollectionList.of(locationCollection)
        .and(locationRecordsOfSubEvents)
        .apply("Joining location records for inheritance", Flatten.pCollections())
        .apply(
            "Inherit location fields of all records",
            Combine.perKey(new LocationInheritedFieldsFn()));
  }

  PCollection<KV<String, TemporalInheritedRecord>> inheritTemporalFields() {
    PCollection<KV<String, TemporalRecord>> temporalRecordsOfSubEvents =
        ParentEventExpandTransform.createTemporalTransform(
                temporalTransform.getTag(),
                eventCoreTransform.getTag(),
                temporalTransform.getEdgeTag())
            .toSubEventsRecordsFromLeaf("Temporal", temporalCollection, eventCoreCollection);

    return PCollectionList.of(temporalCollection)
        .and(temporalRecordsOfSubEvents)
        .apply("Joining temporal records for inheritance", Flatten.pCollections())
        .apply(
            "Inherit temporal fields of all records",
            Combine.perKey(new TemporalInheritedFieldsFn()));
  }

  PCollection<KV<String, EventInheritedRecord>> inheritEventFields() {
    PCollection<KV<String, Edge<EventCoreRecord>>> parentEdgeEvents =
        eventCoreCollection
            // Collection of EventCoreRecord
            .apply("Get EventCoreRecord values", Values.create())
            // Collection of KV<ParentId,Edge.of(ParentId,EventCoreRecord.id, EventCoreRecord)
            .apply(
                "Group by child and parent", inheritedFieldsTransform.childToParentEdgeConverter())
            .setCoder(AvroKvCoder.ofEdge(EventCoreRecord.class));

    return KeyedPCollectionTuple.of(eventCoreTransform.getTag(), eventCoreCollection)
        .and(eventCoreTransform.getEdgeTag(), parentEdgeEvents)
        // Join EventCore collections with parents
        .apply("Join events with parent collections", CoGroupByKey.<String>create())
        // Extract the parents only
        .apply(
            "Extract the parents only",
            inheritedFieldsTransform.childToParentConverter(eventCoreTransform))
        .apply("Extract parent features", Combine.perKey(new EventInheritedFieldsFn()))
        .setCoder(AvroKvCoder.of(EventInheritedRecord.class));
  }
}
