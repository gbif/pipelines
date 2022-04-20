package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AMPLIFICATION_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.AmplificationTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.ggbn.AmplificationTable;

public class AmplificationTableTransform extends TableTransform<AmplificationTable> {

  @Builder
  public AmplificationTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        AmplificationTable.class,
        AMPLIFICATION_TABLE,
        AmplificationTableTransform.class.getName(),
        AMPLIFICATION_TABLE_RECORDS_COUNT,
        AmplificationTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
