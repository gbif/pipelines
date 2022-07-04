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
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;

@Data
public class LocationInheritedFieldsFn
    extends Combine.CombineFn<
        LocationRecord, LocationInheritedFieldsFn.Accum, LocationInheritedRecord> {

  private static final TupleTag<LocationInheritedRecord> TAG =
      new TupleTag<LocationInheritedRecord>() {};

  @Data
  public static class Accum implements Serializable {

    private Map<String, LocationRecord> recordsMap = new HashMap<>();
    private Set<String> recordsWithChildren = new HashSet<>();

    public Accum acc(Set<LocationRecord> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(LocationRecord r) {
      recordsMap.put(r.getId(), r);
      if (r.getParentId() != null) {
        recordsWithChildren.add(r.getParentId());
      }
      return this;
    }

    public LocationInheritedRecord toLeafChild() {
      Set<String> allRecords = new HashSet<>(recordsMap.keySet());
      allRecords.removeAll(recordsWithChildren);
      LocationRecord leaf = recordsMap.get(allRecords.iterator().next());
      return setParentValue(
              LocationInheritedRecord.newBuilder().setId(leaf.getId()),
              recordsMap.get(leaf.getParentId()))
          .build();
    }

    private LocationInheritedRecord.Builder setParentValue(
        LocationInheritedRecord.Builder locationInherited, LocationRecord parent) {
      return setParentValue(locationInherited, parent, false);
    }

    private LocationInheritedRecord.Builder setParentValue(
        LocationInheritedRecord.Builder locationInherited,
        LocationRecord parent,
        boolean assigned) {

      if (assigned || parent == null) {
        return locationInherited;
      }

      if (parent.getCountryCode() != null) {
        locationInherited.setCountryCode(parent.getCountryCode());
        assigned = true;
      }

      if (parent.getStateProvince() != null) {
        locationInherited.setStateProvince(parent.getStateProvince());
        assigned = true;
      }

      if (parent.getHasCoordinate() != null && parent.getHasCoordinate()) {
        locationInherited.setDecimalLatitude(parent.getDecimalLatitude());
        locationInherited.setDecimalLongitude(parent.getDecimalLongitude());
        assigned = true;
      }

      return setParentValue(locationInherited, recordsMap.get(parent.getParentId()), assigned);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, LocationRecord input) {
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
  public LocationInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<LocationInheritedRecord> tag() {
    return TAG;
  }
}
