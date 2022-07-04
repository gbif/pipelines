package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

@Data
public class TemporalInheritedFieldsFn
    extends Combine.CombineFn<
        TemporalRecord, TemporalInheritedFieldsFn.Accum, TemporalInheritedRecord> {

  private static final TupleTag<TemporalRecord> TAG = new TupleTag<TemporalRecord>() {};

  @Data
  public static class Accum implements Serializable {

    private Map<String, TemporalRecord> recordsMap = new HashMap<>();
    private Set<String> recordsWithChildren = new HashSet<>();

    public Accum acc(Set<TemporalRecord> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(TemporalRecord r) {
      recordsMap.put(r.getId(), r);
      if (r.getParentId() != null) {
        recordsWithChildren.add(r.getParentId());
      }
      return this;
    }

    public TemporalInheritedRecord toLeafChild() {
      Set<String> allRecords = new HashSet<>(recordsMap.keySet());
      allRecords.removeAll(recordsWithChildren);
      TemporalRecord leaf = recordsMap.get(allRecords.iterator().next());
      return setParentValue(
              TemporalInheritedRecord.newBuilder().setId(leaf.getId()),
              recordsMap.get(leaf.getParentId()))
          .build();
    }

    private TemporalInheritedRecord.Builder setParentValue(
        TemporalInheritedRecord.Builder temporalInherited, TemporalRecord parent) {
      return setParentValue(temporalInherited, parent, false);
    }

    private TemporalInheritedRecord.Builder setParentValue(
        TemporalInheritedRecord.Builder temporalInherited,
        TemporalRecord parent,
        boolean assigned) {

      if (assigned || parent == null) {
        return temporalInherited;
      }

      if (parent.getYear() != null) {
        temporalInherited.setYear(parent.getYear());
        assigned = true;
      }

      if (parent.getMonth() != null) {
        temporalInherited.setMonth(parent.getMonth());
        assigned = true;
      }

      return setParentValue(temporalInherited, recordsMap.get(parent.getParentId()), assigned);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, TemporalRecord input) {
    return mutableAccumulator.acc(input);
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    return StreamSupport.stream(accumulators.spliterator(), false)
        .reduce(
            new Accum(),
            (acc1, acc2) ->
                new Accum()
                    .acc(new HashSet<>(acc1.getRecordsMap().values()))
                    .acc(new HashSet<>(acc2.getRecordsMap().values())));
  }

  @Override
  public TemporalInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<TemporalRecord> tag() {
    return TAG;
  }
}
