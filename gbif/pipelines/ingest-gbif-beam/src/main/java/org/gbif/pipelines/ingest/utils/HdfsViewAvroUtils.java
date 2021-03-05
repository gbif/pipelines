package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewAvroUtils {

  /**
   * Copies all occurrence records into the directory from targetPath. Deletes pre-existing data of
   * the dataset being processed.
   */
  public static void move(InterpretationPipelineOptions options) {
    if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(OCCURRENCE.name())) {
      moveOccurrence(options);
    } else {
      moveAll(options);
    }
  }

  private static void moveOccurrence(InterpretationPipelineOptions options) {
    move(options, OCCURRENCE);
  }

  private static void moveAll(InterpretationPipelineOptions options) {
    move(options, OCCURRENCE);
    move(options, RecordType.MEASUREMENT_OR_FACT_TABLE, Extension.MEASUREMENT_OR_FACT);
    move(options, RecordType.IDENTIFICATION_TABLE, Extension.IDENTIFICATION);
    move(options, RecordType.RESOURCE_RELATIONSHIP_TABLE, Extension.RESOURCE_RELATIONSHIP);
    move(options, RecordType.AMPLIFICATION_TABLE, Extension.AMPLIFICATION);
    move(options, RecordType.CLONING_TABLE, Extension.CLONING);
    move(options, RecordType.GEL_IMAGE_TABLE, Extension.GEL_IMAGE);
    move(options, RecordType.LOAN_TABLE, Extension.LOAN);
    move(options, RecordType.MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE);
    move(options, RecordType.PERMIT_TABLE, Extension.PERMIT);
    move(options, RecordType.PREPARATION_TABLE, Extension.PREPARATION);
    move(options, RecordType.PRESERVATION_TABLE, Extension.PRESERVATION);
    move(
        options,
        RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE,
        Extension.GERMPLASM_MEASUREMENT_SCORE);
    move(
        options,
        RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRAIT);
    move(
        options,
        RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRIAL);
    move(options, RecordType.GERMPLASM_ACCESSION_TABLE, Extension.GERMPLASM_ACCESSION);
    move(
        options,
        RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        Extension.EXTENDED_MEASUREMENT_OR_FACT);
    move(options, RecordType.CHRONOMETRIC_AGE_TABLE, Extension.CHRONOMETRIC_AGE);
    move(options, RecordType.CHRONOMETRIC_DATE_TABLE, Extension.CHRONOMETRIC_DATE);
    move(options, RecordType.REFERENCE_TABLE, Extension.REFERENCE);
    move(options, RecordType.IDENTIFIER_TABLE, Extension.IDENTIFIER);
  }

  private static void move(InterpretationPipelineOptions options, RecordType recordType) {
    String path = recordType.name().toLowerCase();
    move(options, path, path);
  }

  private static void move(
      InterpretationPipelineOptions options, RecordType recordType, Extension extension) {
    String from = recordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replaceAll("_", "") + "table";
    move(options, from, to);
  }

  private static void move(InterpretationPipelineOptions options, String from, String to) {
    String targetPath = options.getTargetPath();

    String deletePath =
        PathBuilder.buildPath(targetPath, to, options.getDatasetId() + "_*").toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);

    String filter = PathBuilder.buildFilePathViewUsingInputPath(options, from, "*.avro");

    String movePath = PathBuilder.buildPath(targetPath, to).toString();
    log.info("Moving files with pattern {} to {}", filter, movePath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), movePath, filter);
    log.info("Files moved to {} directory", movePath);
  }
}
