package org.gbif.pipelines.assembling.interpretation.steps;

import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Service;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.record.InterpretedExtendedRecordTransform;
import org.gbif.pipelines.transform.record.LocationRecordTransform;
import org.gbif.pipelines.transform.record.MultimediaRecordTransform;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;

import java.nio.file.Paths;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;

/**
 * {@link Supplier} that supplies a {@link InterpretationStep}.
 */
public interface InterpretationStepSupplier extends Supplier<InterpretationStep> {

  /**
   * Gbif location interpretation.
   * <p>
   * It creates the ws config inside the supplier in order to postpone as much as possible, since this step is not
   * required and may never be used.
   */
  static InterpretationStepSupplier locationGbif(
    PipelineTargetPaths paths, CodecFactory avroCodec, String wsProperties
  ) {
    return () -> {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(wsProperties), "ws properties are required");

      // create ws config
      Config wsConfig = HttpConfigFactory.createConfig(Service.GEO_CODE, Paths.get(wsProperties));

      return locationGbif(paths, avroCodec, wsConfig).get();
    };
  }

  /**
   * Gbif location interpretation from {@link Config}.
   */
  static InterpretationStepSupplier locationGbif(
    PipelineTargetPaths paths, CodecFactory avroCodec, Config wsConfig
  ) {
    return () -> InterpretationStep.<LocationRecord>newBuilder().interpretationType(InterpretationType.LOCATION)
      .avroClass(LocationRecord.class)
      .transform(LocationRecordTransform.create(wsConfig))
      .dataTargetPath(paths.getDataTargetPath())
      .issuesTargetPath(paths.getIssuesTargetPath())
      .tempDirectory(paths.getTempDir())
      .avroCodec(avroCodec)
      .build();
  }

  /**
   * Gbif temporal interpretation.
   */
  static InterpretationStepSupplier temporalGbif(PipelineTargetPaths paths, CodecFactory avroCodec) {
    return () -> InterpretationStep.<TemporalRecord>newBuilder().interpretationType(InterpretationType.TEMPORAL)
      .avroClass(TemporalRecord.class)
      .transform(TemporalRecordTransform.create())
      .dataTargetPath(paths.getDataTargetPath())
      .issuesTargetPath(paths.getIssuesTargetPath())
      .tempDirectory(paths.getTempDir())
      .avroCodec(avroCodec)
      .build();
  }

  /**
   * Gbif taxonomy interpretation.
   * <p>
   * It creates the ws config inside the supplier in order to postpone as much as possible, since this step is not
   * required and may never be used.
   */
  static InterpretationStepSupplier taxonomyGbif(
    PipelineTargetPaths paths, CodecFactory avroCodec, String wsProperties
  ) {
    return () -> {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(wsProperties), "ws properties are required");

      // create ws config
      Config wsConfig = HttpConfigFactory.createConfig(Service.SPECIES_MATCH2, Paths.get(wsProperties));

      return taxonomyGbif(paths, avroCodec, wsConfig).get();
    };
  }

  /**
   * Gbif taxonomy interpretation from {@link Config}.
   */
  static InterpretationStepSupplier taxonomyGbif(PipelineTargetPaths paths, CodecFactory avroCodec, Config wsConfig) {
    return () -> InterpretationStep.<TaxonRecord>newBuilder().interpretationType(InterpretationType.TAXONOMY)
      .avroClass(TaxonRecord.class)
      .transform(TaxonRecordTransform.create(wsConfig))
      .dataTargetPath(paths.getDataTargetPath())
      .issuesTargetPath(paths.getIssuesTargetPath())
      .tempDirectory(paths.getTempDir())
      .avroCodec(avroCodec)
      .build();
  }

  /**
   * Gbif common interpretations.
   */
  static InterpretationStepSupplier commonGbif(PipelineTargetPaths paths, CodecFactory avroCodec) {
    return () -> InterpretationStep.<InterpretedExtendedRecord>newBuilder().interpretationType(InterpretationType.COMMON)
      .avroClass(InterpretedExtendedRecord.class)
      .transform(InterpretedExtendedRecordTransform.create())
      .dataTargetPath(paths.getDataTargetPath())
      .issuesTargetPath(paths.getIssuesTargetPath())
      .tempDirectory(paths.getTempDir())
      .avroCodec(avroCodec)
      .build();
  }

  /**
   * Gbif multimedia interpretations.
   */
  static InterpretationStepSupplier multimediaGbif(PipelineTargetPaths paths, CodecFactory avroCodec) {
    return () -> InterpretationStep.<MultimediaRecord>newBuilder().interpretationType(InterpretationType.MULTIMEDIA)
      .avroClass(MultimediaRecord.class)
      .transform(MultimediaRecordTransform.create())
      .dataTargetPath(paths.getDataTargetPath())
      .issuesTargetPath(paths.getIssuesTargetPath())
      .tempDirectory(paths.getTempDir())
      .avroCodec(avroCodec)
      .build();
  }
}
