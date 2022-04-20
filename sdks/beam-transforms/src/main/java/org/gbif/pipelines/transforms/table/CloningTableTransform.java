package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CLONING_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CLONING_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.CloningTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.ggbn.CloningTable;

public class CloningTableTransform extends TableTransform<CloningTable> {

  @Builder
  public CloningTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        CloningTable.class,
        CLONING_TABLE,
        CloningTableTransform.class.getName(),
        CLONING_TABLE_RECORDS_COUNT,
        CloningTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
