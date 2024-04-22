package org.gbif.pipelines.transforms.table;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.MULTIMEDIA_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_TABLE_RECORDS_COUNT;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.MultimediaTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.gbif.MultimediaTable;

public class MultimediaTableTransform extends TableTransform<MultimediaTable> {

  @Builder
  public MultimediaTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Set<String> types) {
    super(
        MultimediaTable.class,
        MULTIMEDIA_TABLE,
        MultimediaTableTransform.class.getName(),
        MULTIMEDIA_TABLE_RECORDS_COUNT,
        MultimediaTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setTypes(types);
  }
}
