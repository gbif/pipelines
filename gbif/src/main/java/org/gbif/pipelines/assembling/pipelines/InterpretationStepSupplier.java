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

/**
 * {@link Supplier} that supplies a {@link InterpretationStep}.
 */
public interface InterpretationStepSupplier extends Supplier<InterpretationStep> {

  /**
   * Gbif location interpretation.
   */
  static InterpretationStepSupplier locationGbif(PipelineTargetPaths paths) {
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

  /**
   * Gbif temporal interpretation.
   */
  static InterpretationStepSupplier temporalGbif(PipelineTargetPaths paths) {
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

  /**
   * Gbif taxonomy interpretation.
   */
  static InterpretationStepSupplier taxonomyGbif(PipelineTargetPaths paths) {
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

  /**
   * Gbif common interpretations.
   */
  static InterpretationStepSupplier commonGbif(PipelineTargetPaths paths) {
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
