package org.gbif.pipelines.assembling.interpretation.steps;

import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationTransform;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link Supplier} that supplies a {@link InterpretationStep}.
 */
public interface InterpretationStepSupplier extends Supplier<InterpretationStep> {

  /**
   * Gbif location interpretation.
   */
  static InterpretationStepSupplier locationGbif(Function<InterpretationType, PipelineTargetPaths> pathsGenerator) {
    return () -> InterpretationStep.<Location>newBuilder().interpretationType(InterpretationType.LOCATION)
      .avroClass(Location.class)
      .transform(LocationTransform.create())
      .pathsGenerator(pathsGenerator)
      .build();
  }

  /**
   * Gbif temporal interpretation.
   */
  static InterpretationStepSupplier temporalGbif(Function<InterpretationType, PipelineTargetPaths> pathsGenerator) {
    return () -> InterpretationStep.<TemporalRecord>newBuilder().interpretationType(InterpretationType.TEMPORAL)
      .avroClass(TemporalRecord.class)
      .transform(TemporalRecordTransform.create())
      .pathsGenerator(pathsGenerator)
      .build();
  }

  /**
   * Gbif taxonomy interpretation.
   */
  static InterpretationStepSupplier taxonomyGbif(Function<InterpretationType, PipelineTargetPaths> pathsGenerator) {
    return () -> InterpretationStep.<TaxonRecord>newBuilder().interpretationType(InterpretationType.TAXONOMY)
      .avroClass(TaxonRecord.class)
      .transform(TaxonRecordTransform.create())
      .pathsGenerator(pathsGenerator)
      .build();
  }

  /**
   * Gbif common interpretations.
   */
  static InterpretationStepSupplier commonGbif(Function<InterpretationType, PipelineTargetPaths> pathsGenerator) {
    return () -> InterpretationStep.<InterpretedExtendedRecord>newBuilder().interpretationType(InterpretationType.COMMON)
      .avroClass(InterpretedExtendedRecord.class)
      .transform(InterpretedExtendedRecordTransform.create())
      .pathsGenerator(pathsGenerator)
      .build();
  }
}
