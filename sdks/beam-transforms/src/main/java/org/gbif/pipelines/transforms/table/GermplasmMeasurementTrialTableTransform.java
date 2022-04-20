package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE;

import java.util.Set;
import lombok.Builder;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.GermplasmMeasurementTrialTableConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTrialTable;

public class GermplasmMeasurementTrialTableTransform
    extends TableTransform<GermplasmMeasurementTrialTable> {

  @Builder
  public GermplasmMeasurementTrialTableTransform(
      TupleTag<ExtendedRecord> extendedRecordTag,
      TupleTag<GbifIdRecord> gbifIdRecordTag,
      String path,
      Integer numShards,
      Set<String> types) {
    super(
        GermplasmMeasurementTrialTable.class,
        GERMPLASM_MEASUREMENT_TRIAL_TABLE,
        GermplasmMeasurementTrialTableTransform.class.getName(),
        MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT,
        GermplasmMeasurementTrialTableConverter::convert);
    this.setExtendedRecordTag(extendedRecordTag)
        .setGbifIdRecordTag(gbifIdRecordTag)
        .setPath(path)
        .setNumShards(numShards)
        .setTypes(types);
  }
}
