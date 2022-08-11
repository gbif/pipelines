package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;

@Data
public class EventInheritedFieldsFn
    extends Combine.CombineFn<EventCoreRecord, EventInheritedFieldsFn.Accum, EventInheritedRecord> {

  private static final TupleTag<EventCoreRecord> TAG = new TupleTag<EventCoreRecord>() {};

  @Data
  public static class Accum implements Serializable {

    private Map<String, EventInheritedFields> recordsMap = new HashMap<>();
    private Set<String> recordsWithChildren = new HashSet<>();

    public Accum acc(Set<EventCoreRecord> records) {
      records.forEach(r -> acc(EventInheritedFields.from(r)));
      return this;
    }

    public Accum accInheritedFields(Set<EventInheritedFields> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(EventInheritedFields r) {
      recordsMap.put(r.getId(), r);
      Optional.ofNullable(r.getParentEventID()).ifPresent(recordsWithChildren::add);
      return this;
    }

    public EventInheritedRecord toLeafChild() {
      ArrayDeque<String> allRecords = new ArrayDeque<>(recordsMap.keySet());
      allRecords.removeAll(recordsWithChildren);
      EventInheritedFields leaf = recordsMap.get(allRecords.peek());

      EventInheritedRecord eventInheritedRecord =
          setParentValues(
                  EventInheritedRecord.newBuilder()
                      .setId(leaf.getId())
                      .setEventType(leaf.getEventTypes()),
                  leaf.getParentEventID(),
                  leaf.locationID != null)
              .build();

      if (eventInheritedRecord.getLocationID() == null
          && (eventInheritedRecord.getEventType() == null
              || eventInheritedRecord.getEventType().isEmpty())) {
        return EventInheritedRecord.newBuilder().build();
      }

      return eventInheritedRecord;
    }

    private EventInheritedRecord.Builder setParentValues(
        EventInheritedRecord.Builder builder, String parentId, boolean assigned) {
      if (assigned || parentId == null) {
        return builder;
      }

      EventInheritedFields parent = recordsMap.get(parentId);

      if (parent.getLocationID() != null) {
        builder.setLocationID(parent.getLocationID());
        builder.setInheritedFrom(parent.getId());
        assigned = true;
      }

      return setParentValues(builder, parent.getParentEventID(), assigned);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, EventCoreRecord input) {
    return mutableAccumulator.acc(EventInheritedFields.from(input));
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    return StreamSupport.stream(accumulators.spliterator(), false)
        .reduce(
            new Accum(),
            (acc1, acc2) ->
                new Accum()
                    .accInheritedFields(new HashSet<>(acc1.getRecordsMap().values()))
                    .accInheritedFields(new HashSet<>(acc2.getRecordsMap().values())));
  }

  @Override
  public EventInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<EventCoreRecord> tag() {
    return TAG;
  }

  @Data
  static class EventInheritedFields implements Serializable {

    private String id;
    private String parentEventID;
    private String locationID;
    private List<String> eventTypes;

    static EventInheritedFields from(EventCoreRecord eventCoreRecord) {
      EventInheritedFields eif = new EventInheritedFields();
      eif.id = eventCoreRecord.getId();
      eif.parentEventID = eventCoreRecord.getParentEventID();
      eif.locationID = eventCoreRecord.getLocationID();

      if (eventCoreRecord.getParentsLineage() != null
          && !eventCoreRecord.getParentsLineage().isEmpty()) {
        eif.eventTypes =
            eventCoreRecord.getParentsLineage().stream()
                .filter(p -> p.getEventType() != null)
                .map(Parent::getEventType)
                .collect(Collectors.toList());
      }
      return eif;
    }
  }
}
