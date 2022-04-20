package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ExtendedMeasurementOrFactTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.obis.ExtendedMeasurementOrFactTable;

public class ExtendedMeasurementOrFactTableTransform
    extends TableTransform<ExtendedMeasurementOrFactTable> {

  @Builder
  public ExtendedMeasurementOrFactTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        ExtendedMeasurementOrFactTable.class,
        EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        ExtendedMeasurementOrFactTableTransform.class.getName(),
        EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT,
        ExtendedMeasurementOrFactTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
