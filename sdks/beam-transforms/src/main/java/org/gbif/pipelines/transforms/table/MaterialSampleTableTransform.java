package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MATERIAL_SAMPLE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MATERIAL_SAMPLE_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.MaterialSampleTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.extension.ggbn.MaterialSampleTable;

public class MaterialSampleTableTransform extends TableTransform<MaterialSampleTable> {

  @Builder
  public MaterialSampleTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        MaterialSampleTable.class,
        MATERIAL_SAMPLE_TABLE,
        MaterialSampleTableTransform.class.getName(),
        MATERIAL_SAMPLE_TABLE_RECORDS_COUNT,
        MaterialSampleTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
