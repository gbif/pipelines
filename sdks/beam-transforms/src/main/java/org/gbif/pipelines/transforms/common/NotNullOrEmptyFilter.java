package org.gbif.pipelines.transforms.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Beam function to filter Avro records that contain a non-null and not empty field. */
@Data
@AllArgsConstructor(staticName = "of")
public class NotNullOrEmptyFilter<T extends SpecificRecordBase>
    implements SerializableFunction<T, Boolean> {

  private final SerializableFunction<T, String> fieldMapper;

  @Override
  public Boolean apply(T input) {
    String fieldValue = fieldMapper.apply(input);
    return fieldValue != null && !fieldValue.isEmpty();
  }
}
