package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_AGE_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ChronometricAgeTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.dwc.ChronometricAgeTable;

public class ChronometricAgeTableTransform extends TableTransform<ChronometricAgeTable> {

  @Builder
  public ChronometricAgeTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        ChronometricAgeTable.class,
        CHRONOMETRIC_AGE_TABLE,
        ChronometricAgeTableTransform.class.getName(),
        CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT,
        ChronometricAgeTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
