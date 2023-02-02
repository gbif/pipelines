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

    private Map<String, LocationInheritedFields> recordsMap = new HashMap<>();
    private Set<String> recordsWithChildren = new HashSet<>();

    public Accum acc(Set<LocationRecord> records) {
      records.forEach(r -> acc(LocationInheritedFields.from(r)));
      return this;
    }

    public Accum accInheritedFields(Set<LocationInheritedFields> records) {
      records.forEach(this::acc);
      return this;
    }

    public Accum acc(LocationInheritedFields r) {
      recordsMap.put(r.getId(), r);
      Optional.ofNullable(r.getParentId()).ifPresent(recordsWithChildren::add);
      return this;
    }

    private LocationInheritedFields getLeafChild() {
      ArrayDeque<String> allRecords = new ArrayDeque<>(recordsMap.keySet());
      allRecords.removeAll(recordsWithChildren);
      return recordsMap.get(allRecords.peek());
    }

    public LocationInheritedRecord toLeafChild() {
      return inheritFields(getLeafChild());
    }

    private LocationInheritedRecord inheritFields(LocationInheritedFields leaf) {
      LocationInheritedRecord.Builder builder = LocationInheritedRecord.newBuilder();

      if (leaf.allFieldsNull()) {
        boolean assignedInheritedFields = setParentValues(builder, leaf.getParentId(), false);

        if (assignedInheritedFields) {
          builder.setId(leaf.getId());
        }
      }

      return builder.build();
    }

    private boolean setParentValues(
        LocationInheritedRecord.Builder builder, String parentId, boolean assigned) {

      if (assigned || parentId == null) {
        return assigned;
      }

      LocationInheritedFields parent = recordsMap.get(parentId);

      if (parent == null) {
        return assigned;
      }

      if (parent.getCountryCode() != null) {
        builder.setCountryCode(parent.getCountryCode());
        assigned = true;
      }

      if (parent.getStateProvince() != null) {
        builder.setStateProvince(parent.getStateProvince());
        assigned = true;
      }

      if (parent.getHasCoordinate() != null && parent.getHasCoordinate()) {
        builder.setDecimalLatitude(parent.getDecimalLatitude());
        builder.setDecimalLongitude(parent.getDecimalLongitude());
        assigned = true;
      }

      if (assigned) {
        builder.setInheritedFrom(parent.getId());
      }

      return setParentValues(builder, parent.getParentId(), assigned);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, LocationRecord input) {
    return mutableAccumulator.acc(LocationInheritedFields.from(input));
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
  public LocationInheritedRecord extractOutput(Accum accumulator) {
    return accumulator.toLeafChild();
  }

  public static TupleTag<LocationInheritedRecord> tag() {
    return TAG;
  }

  @Data
  static class LocationInheritedFields implements Serializable {

    private String id;
    private String parentId;
    private String countryCode;
    private String stateProvince;
    private Boolean hasCoordinate;
    private Double decimalLatitude;
    private Double decimalLongitude;

    static LocationInheritedFields from(LocationRecord locationRecord) {
      LocationInheritedFields lif = new LocationInheritedFields();
      lif.id = locationRecord.getId();
      lif.parentId = locationRecord.getParentId();
      lif.countryCode = locationRecord.getCountryCode();
      lif.stateProvince = locationRecord.getStateProvince();
      lif.decimalLatitude = locationRecord.getDecimalLatitude();
      lif.decimalLongitude = locationRecord.getDecimalLongitude();
      lif.hasCoordinate = locationRecord.getHasCoordinate();
      return lif;
    }

    boolean allFieldsNull() {
      return countryCode == null
          && stateProvince == null
          && decimalLatitude == null
          && decimalLongitude == null;
    }
  }
}
