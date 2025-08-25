package org.gbif.pipelines.transforms.table;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.HUMBOLDT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.HUMBOLDT_TABLE_RECORDS_COUNT;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.HumboldtTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.eco.HumboldtTable;

public class HumboldtTableTransform extends TableTransform<HumboldtTable> {

  @Builder
  public HumboldtTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        HumboldtTable.class,
        HUMBOLDT_TABLE,
        HumboldtTableTransform.class.getName(),
        HUMBOLDT_TABLE_RECORDS_COUNT,
        HumboldtTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
