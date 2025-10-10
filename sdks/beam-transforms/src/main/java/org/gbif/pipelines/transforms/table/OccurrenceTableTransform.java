package org.gbif.pipelines.transforms.table;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.OCCURRENCE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_TABLE_RECORDS_COUNT;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.OccurrenceTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.dwc_occurrence.xml.OccurrenceTable;

public class OccurrenceTableTransform extends TableTransform<OccurrenceTable> {

  @Builder
  public OccurrenceTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<IdentifierRecord> identifierRecordTag,
      PCollectionView<MetadataRecord> metadataView,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        OccurrenceTable.class,
        OCCURRENCE_TABLE,
        OccurrenceTableTransform.class.getName(),
        OCCURRENCE_EXT_TABLE_RECORDS_COUNT,
        OccurrenceTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setIdentifierRecordTag(identifierRecordTag)
        .setMetadataRecord(metadataView)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
