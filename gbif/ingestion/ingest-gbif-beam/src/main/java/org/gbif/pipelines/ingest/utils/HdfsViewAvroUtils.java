package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EVENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
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
    } else if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(EVENT.name())) {
      move(options, EVENT);
    } else {
      moveAll(options);
    }
  }

  private static void moveOccurrence(InterpretationPipelineOptions options) {
    move(options, OCCURRENCE);
  }

  private static void moveTables(
      InterpretationPipelineOptions options, RecordType interpretationType) {
    move(
        options,
        interpretationType,
        RecordType.MEASUREMENT_OR_FACT_TABLE,
        Extension.MEASUREMENT_OR_FACT);
    move(options, interpretationType, RecordType.IDENTIFICATION_TABLE, Extension.IDENTIFICATION);
    move(
        options,
        interpretationType,
        RecordType.RESOURCE_RELATIONSHIP_TABLE,
        Extension.RESOURCE_RELATIONSHIP);
    move(options, interpretationType, RecordType.AMPLIFICATION_TABLE, Extension.AMPLIFICATION);
    move(options, interpretationType, RecordType.CLONING_TABLE, Extension.CLONING);
    move(options, interpretationType, RecordType.GEL_IMAGE_TABLE, Extension.GEL_IMAGE);
    move(options, interpretationType, RecordType.LOAN_TABLE, Extension.LOAN);
    move(options, interpretationType, RecordType.MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE);
    move(options, interpretationType, RecordType.PERMIT_TABLE, Extension.PERMIT);
    move(options, interpretationType, RecordType.PREPARATION_TABLE, Extension.PREPARATION);
    move(options, interpretationType, RecordType.PRESERVATION_TABLE, Extension.PRESERVATION);
    move(
        options,
        interpretationType,
        RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE,
        Extension.GERMPLASM_MEASUREMENT_SCORE);
    move(
        options,
        interpretationType,
        RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRAIT);
    move(
        options,
        interpretationType,
        RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRIAL);
    move(
        options,
        interpretationType,
        RecordType.GERMPLASM_ACCESSION_TABLE,
        Extension.GERMPLASM_ACCESSION);
    move(
        options,
        interpretationType,
        RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        Extension.EXTENDED_MEASUREMENT_OR_FACT);
    move(
        options, interpretationType, RecordType.CHRONOMETRIC_AGE_TABLE, Extension.CHRONOMETRIC_AGE);
    move(
        options,
        interpretationType,
        RecordType.CHRONOMETRIC_DATE_TABLE,
        Extension.CHRONOMETRIC_DATE);
    move(options, interpretationType, RecordType.REFERENCE_TABLE, Extension.REFERENCE);
    move(options, interpretationType, RecordType.IDENTIFIER_TABLE, Extension.IDENTIFIER);
    move(options, interpretationType, RecordType.AUDUBON_TABLE, Extension.AUDUBON);
    move(options, interpretationType, RecordType.IMAGE_TABLE, Extension.IMAGE);
    move(options, interpretationType, RecordType.MULTIMEDIA_TABLE, Extension.MULTIMEDIA);
    move(
        options, interpretationType, RecordType.DNA_DERIVED_DATA_TABLE, Extension.DNA_DERIVED_DATA);
  }

  private static void moveAll(InterpretationPipelineOptions options) {
    if (options.getCoreRecordType() == OCCURRENCE) {
      move(options, OCCURRENCE);
      moveTables(options, OCCURRENCE);
    } else if (options.getCoreRecordType() == EVENT) {
      move(options, EVENT);
      moveTables(options, EVENT);
    }
  }

  private static void move(InterpretationPipelineOptions options, RecordType recordType) {
    String path = recordType.name().toLowerCase();
    move(options, recordType, path, path);
  }

  private static void move(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Extension extension) {
    String from = extensionRecordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replace("_", "") + "table";
    move(options, recordType, from, to);
  }

  private static void move(
      InterpretationPipelineOptions options, RecordType recordType, String from, String to) {
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String deletePath =
        PathBuilder.buildPath(
                targetPath, recordType.name().toLowerCase(), to, options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(hdfsConfigs, targetPath, deletePath);

    String filter =
        PathBuilder.buildFilePathViewUsingInputPath(options, recordType, from, "*.avro");

    String movePath =
        PathBuilder.buildPath(targetPath, recordType.name().toLowerCase(), to).toString();
    log.info("Moving files with pattern {} to {}", filter, movePath);
    FsUtils.moveDirectory(hdfsConfigs, movePath, filter);
    log.info("Files moved to {} directory", movePath);
  }
}
