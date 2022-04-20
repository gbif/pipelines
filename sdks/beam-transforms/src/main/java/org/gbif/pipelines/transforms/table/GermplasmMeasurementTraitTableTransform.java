package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTraitTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTraitTable;

public class GermplasmMeasurementTraitTableTransform
    extends TableTransform<GermplasmMeasurementTraitTable> {

  @Builder
  public GermplasmMeasurementTraitTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        GermplasmMeasurementTraitTable.class,
        GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        GermplasmMeasurementTraitTableTransform.class.getName(),
        MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT,
        GermplasmMeasurementTraitTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
