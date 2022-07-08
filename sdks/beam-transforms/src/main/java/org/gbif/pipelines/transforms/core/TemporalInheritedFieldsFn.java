package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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

    private Map<String, TemporalInheritedFields> recordsMap = new HashMap<>();
    private Set<String> recordsWithChildren = new HashSet<>();

    public Accum acc(Set<TemporalRecord> records) {
      records.stream().map(TemporalInheritedFields::from).forEach(this::acc);
      return this;
    }

    public Accum accInheritedFields(Set<TemporalInheritedFields> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(TemporalInheritedFields r) {
      recordsMap.put(r.getId(), r);
      Optional.ofNullable(r.getParentId()).ifPresent(recordsWithChildren::add);
      return this;
    }

    private TemporalInheritedFields getLeafChild() {
      ArrayDeque<String> allRecords = new ArrayDeque<>(recordsMap.keySet());
      allRecords.removeAll(recordsWithChildren);
      return recordsMap.get(allRecords.peek());
    }

    public TemporalInheritedRecord toLeafChild() {
      return setParentValue(getLeafChild()).build();
    }

    private TemporalInheritedRecord.Builder setParentValue(TemporalInheritedFields leaf) {
      return setParentValue(
          TemporalInheritedRecord.newBuilder().setId(leaf.getId()), leaf.getParentId(), false);
    }

    private TemporalInheritedRecord.Builder setParentValue(
        TemporalInheritedRecord.Builder builder, String parentId, boolean assigned) {
      if (assigned || parentId == null) {
        return builder;
      }

      TemporalInheritedFields parent = recordsMap.get(parentId);

      if (parent.getYear() != null) {
        builder.setYear(parent.getYear());
        assigned = true;
      }

      if (parent.getMonth() != null) {
        builder.setMonth(parent.getMonth());
        assigned = true;
      }

      return setParentValue(builder, parent.getParentId(), assigned);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, TemporalRecord input) {
    return mutableAccumulator.acc(TemporalInheritedFields.from(input));
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
  public TemporalInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<TemporalRecord> tag() {
    return TAG;
  }

  @Data
  public static class TemporalInheritedFields implements Serializable {

    private String id;
    private String parentId;
    private Integer year;
    private Integer month;

    public static TemporalInheritedFields from(TemporalRecord temporalRecord) {
      TemporalInheritedFields tif = new TemporalInheritedFields();
      tif.id = temporalRecord.getId();
      tif.parentId = temporalRecord.getParentId();
      tif.year = temporalRecord.getYear();
      tif.month = temporalRecord.getMonth();
      return tif;
    }
  }
}
