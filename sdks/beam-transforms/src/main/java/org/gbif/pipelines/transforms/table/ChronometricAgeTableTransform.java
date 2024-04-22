package org.gbif.pipelines.transforms.table;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.CHRONOMETRIC_AGE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.ChronometricAgeTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.dwc.ChronometricAgeTable;

public class ChronometricAgeTableTransform extends TableTransform<ChronometricAgeTable> {

  @Builder
  public ChronometricAgeTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Set<String> types) {
    super(
        ChronometricAgeTable.class,
        CHRONOMETRIC_AGE_TABLE,
        ChronometricAgeTableTransform.class.getName(),
        CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT,
        ChronometricAgeTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setTypes(types);
  }
}
