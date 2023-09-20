package org.gbif.pipelines.transforms.table;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.REFERENCE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.REFERENCE_TABLE_RECORDS_COUNT;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ReferenceTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.gbif.ReferenceTable;

public class ReferenceTableTransform extends TableTransform<ReferenceTable> {

  @Builder
  public ReferenceTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        ReferenceTable.class,
        REFERENCE_TABLE,
        ReferenceTableTransform.class.getName(),
        REFERENCE_TABLE_RECORDS_COUNT,
        ReferenceTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
