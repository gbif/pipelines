package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_DATE_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.converters.ChronometricDateTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.ChronometricDateTable;

public class ChronometricDateTableTransform extends TableTransform<ChronometricDateTable> {

  @Builder
  public ChronometricDateTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      SerializableFunction<InterpretationType, String> pathFn,
      Integer numShards,
      Set<String> types) {
    super(
        ChronometricDateTable.class,
        CHRONOMETRIC_DATE_TABLE,
        ChronometricDateTableTransform.class.getName(),
        CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT,
        ChronometricDateTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setBasicRecordTag(basicRecordTag)
        .setPathFn(pathFn)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
