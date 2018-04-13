package org.gbif.pipelines.assembling.pipelines;

import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationTransform;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;

import java.util.function.Supplier;

import org.apache.beam.sdk.PipelineResult;

public interface InterpretationStepSupplier extends Supplier<InterpretationStep> {

  static InterpretationStepSupplier locationGbif(PipelinePaths paths) {
    return () -> {
      InterpretationStep<Location> locationStep =
        InterpretationStep.<Location>newBuilder().interpretationType(InterpretationType.LOCATION)
          .avroClass(Location.class)
          .transform(new LocationTransform())
          .dataTargetPath(paths.getDataTargetPath())
          .issuesTargetPath(paths.getIssuesTargetPath())
          .build();

      return locationStep;
    };
  }

  static InterpretationStepSupplier temporalGbif(PipelinePaths paths) {
    return () -> {
      InterpretationStep<TemporalRecord> temporalStep =
        InterpretationStep.<TemporalRecord>newBuilder().interpretationType(InterpretationType.TEMPORAL)
          .avroClass(TemporalRecord.class)
          .transform(new TemporalRecordTransform())
          .dataTargetPath(paths.getDataTargetPath())
          .issuesTargetPath(paths.getIssuesTargetPath())
          .build();

      return temporalStep;
    };
  }

  static InterpretationStepSupplier taxonomyGbif(PipelinePaths paths) {
    return () -> {
      InterpretationStep<TaxonRecord> taxonomyStep =
        InterpretationStep.<TaxonRecord>newBuilder().interpretationType(InterpretationType.TAXONOMY)
          .avroClass(TaxonRecord.class)
          .transform(new TaxonRecordTransform())
          .dataTargetPath(paths.getDataTargetPath())
          .issuesTargetPath(paths.getIssuesTargetPath())
          .build();

      return taxonomyStep;
    };
  }

  static InterpretationStepSupplier commonGbif(PipelinePaths paths) {
    return () -> {
      InterpretationStep<InterpretedExtendedRecord> interpretedExtendedStep =
        InterpretationStep.<InterpretedExtendedRecord>newBuilder().interpretationType(InterpretationType.COMMON)
          .avroClass(InterpretedExtendedRecord.class)
          .transform(new InterpretedExtendedRecordTransform())
          .dataTargetPath(paths.getDataTargetPath())
          .issuesTargetPath(paths.getIssuesTargetPath())
          .build();

      return interpretedExtendedStep;
    };
  }
}
