package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.converters.ExtendedMeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.ExtendedMeasurementOrFactTable;

public class ExtendedMeasurementOrFactTableTransform
    extends TableTransform<ExtendedMeasurementOrFactTable> {

  @Builder
  public ExtendedMeasurementOrFactTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      SerializableFunction<InterpretationType, String> pathFn,
      Integer numShards,
      Set<String> types) {
    super(
        ExtendedMeasurementOrFactTable.class,
        EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        ExtendedMeasurementOrFactTableTransform.class.getName(),
        EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT,
        ExtendedMeasurementOrFactTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setBasicRecordTag(basicRecordTag)
        .setPathFn(pathFn)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
