package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFICATION_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.converters.IdentificationTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.IdentificationTable;

public class IdentificationTableTransform extends TableTransform<IdentificationTable> {

  @Builder
  public IdentificationTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      SerializableFunction<InterpretationType, String> pathFn,
      Integer numShards,
      Set<String> types) {
    super(
        IdentificationTable.class,
        IDENTIFICATION_TABLE,
        IdentificationTableTransform.class.getName(),
        IDENTIFICATION_TABLE_RECORDS_COUNT,
        IdentificationTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setBasicRecordTag(basicRecordTag)
        .setPathFn(pathFn)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
