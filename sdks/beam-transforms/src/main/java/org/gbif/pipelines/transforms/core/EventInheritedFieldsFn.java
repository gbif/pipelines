package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private List<EventCoreRecord> parents = new ArrayList<>();

    public Accum acc(Set<EventCoreRecord> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(EventCoreRecord r) {
      parents.add(r);
      return this;
    }

    private List<String> getEventTypes() {
      List<String> eventTypes = new ArrayList<>();
      parents.forEach(
          p -> {
            if (p.getEventType() != null && p.getEventType().getConcept() != null) {
              eventTypes.add(p.getEventType().getConcept());
            }
          });
      return eventTypes;
    }

    public EventInheritedRecord toLeafChild() {
      EventCoreRecord leaf = parents.iterator().next();
      EventInheritedRecord inheritedRecord =
          setParentValue(EventInheritedRecord.newBuilder().setId(leaf.getId())).build();
      List<String> eventTypes = getEventTypes();
      if (!eventTypes.isEmpty()) {
        inheritedRecord.setEventType(getEventTypes());
      }
      return inheritedRecord;
    }

    private EventInheritedRecord.Builder setParentValue(
        EventInheritedRecord.Builder eventInherited) {
      Collections.reverse(parents);
      for (EventCoreRecord parent : parents) {
        if (parent.getLocationID() != null) {
          return eventInherited.setLocationID(parent.getLocationID());
        }
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
