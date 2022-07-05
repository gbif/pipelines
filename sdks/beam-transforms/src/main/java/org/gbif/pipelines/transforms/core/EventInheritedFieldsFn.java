package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;

@Data
public class EventInheritedFieldsFn
    extends Combine.CombineFn<EventCoreRecord, EventInheritedFieldsFn.Accum, EventInheritedRecord> {

  private static final TupleTag<EventCoreRecord> TAG = new TupleTag<EventCoreRecord>() {};

  @Data
  public static class Accum implements Serializable {

    private ArrayDeque<EventCoreRecord> parents = new ArrayDeque<>();

    public Accum acc(Set<EventCoreRecord> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(EventCoreRecord r) {
      parents.push(r);
      return this;
    }

    private List<String> getEventTypes() {
      return parents.stream()
          .filter(p -> p.getEventType() != null && p.getEventType().getConcept() != null)
          .map(ecr -> ecr.getEventType().getConcept())
          .collect(Collectors.toList());
    }

    private EventInheritedRecord getLeafRecord() {
      EventCoreRecord leaf = parents.peek();
      return setParentValue(EventInheritedRecord.newBuilder().setId(leaf.getId())).build();
    }

    public EventInheritedRecord toLeafChild() {
      EventInheritedRecord inheritedRecord = getLeafRecord();
      List<String> eventTypes = getEventTypes();
      if (!eventTypes.isEmpty()) {
        inheritedRecord.setEventType(eventTypes);
      }
      return inheritedRecord;
    }

    private Optional<String> getFirstParentLocationId() {
      return parents.stream()
          .filter(p -> p.getLocationID() != null)
          .findFirst()
          .map(EventCoreRecord::getLocationID);
    }

    private EventInheritedRecord.Builder setParentValue(
        EventInheritedRecord.Builder eventInherited) {
      Optional<String> locationId = getFirstParentLocationId();
      if (locationId.isPresent()) {
        return eventInherited.setLocationID(locationId.get());
      }
      return eventInherited;
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, EventCoreRecord input) {
    return mutableAccumulator.acc(input);
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    return StreamSupport.stream(accumulators.spliterator(), false)
        .reduce(
            new Accum(),
            (acc1, acc2) ->
                new Accum()
                    .acc(new HashSet<>(acc1.getParents()))
                    .acc(new HashSet<>(acc2.getParents())));
  }

  @Override
  public EventInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<EventCoreRecord> tag() {
    return TAG;
  }
}
