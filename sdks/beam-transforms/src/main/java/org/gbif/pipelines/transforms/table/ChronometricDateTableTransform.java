package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_DATE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_DATE_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ChronometricDateTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.zooarchnet.ChronometricDateTable;

public class ChronometricDateTableTransform extends TableTransform<ChronometricDateTable> {

  @Builder
  public ChronometricDateTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      String path,
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
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
