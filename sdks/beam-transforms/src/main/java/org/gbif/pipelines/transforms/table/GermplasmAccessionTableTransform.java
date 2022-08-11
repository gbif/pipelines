package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_ACCESSION_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.GermplasmAccessionTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmAccessionTable;

public class GermplasmAccessionTableTransform extends TableTransform<GermplasmAccessionTable> {

  @Builder
  public GermplasmAccessionTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        GermplasmAccessionTable.class,
        GERMPLASM_ACCESSION_TABLE,
        GermplasmAccessionTableTransform.class.getName(),
        GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT,
        GermplasmAccessionTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
