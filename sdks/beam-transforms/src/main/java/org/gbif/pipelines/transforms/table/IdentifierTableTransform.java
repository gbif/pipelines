package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFIER_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.core.converters.IdentifierTableConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.extension.IdentifierTable;

public class IdentifierTableTransform extends TableTransform<IdentifierTable> {

  @Builder
  public IdentifierTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<BasicRecord> basicRecordTag,
      SerializableFunction<InterpretationType, String> pathFn,
      Integer numShards,
      Set<String> types) {
    super(
        IdentifierTable.class,
        IDENTIFIER_TABLE,
        IdentifierTableTransform.class.getName(),
        IDENTIFIER_TABLE_RECORDS_COUNT,
        IdentifierTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setBasicRecordTag(basicRecordTag)
        .setPathFn(pathFn)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
