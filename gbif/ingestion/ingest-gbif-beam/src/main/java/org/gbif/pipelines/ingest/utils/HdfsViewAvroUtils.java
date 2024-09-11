package org.gbif.pipelines.ingest.utils;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.*;
import static org.gbif.api.model.pipelines.InterpretationType.RecordType.EVENT;
import static org.gbif.api.model.pipelines.InterpretationType.RecordType.OCCURRENCE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewAvroUtils {

  /**
   * Cleans empty avro files for extensions and copies all avro files into the directory from
   * targetPath. Deletes pre-existing data of the dataset being processed.
   */
  public static void copyAndOverwrite(InterpretationPipelineOptions options) {
    if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(OCCURRENCE.name())) {
      copyOccurrence(options);
    } else if (options.getInterpretationTypes().size() == 1
        && options.getInterpretationTypes().contains(EVENT.name())) {
      copyAndOverwrite(options, EVENT);
    } else {
      copyAll(options);
    }
  }

  private static void copyOccurrence(InterpretationPipelineOptions options) {
    copyAndOverwrite(options, OCCURRENCE);
  }

  private static void cleanAndMoveTables(InterpretationPipelineOptions opt, RecordType coreType) {

    copyAndOverwrite(opt, coreType, MEASUREMENT_OR_FACT_TABLE, Extension.MEASUREMENT_OR_FACT);
    copyAndOverwrite(opt, coreType, IDENTIFICATION_TABLE, Extension.IDENTIFICATION);
    copyAndOverwrite(opt, coreType, RESOURCE_RELATIONSHIP_TABLE, Extension.RESOURCE_RELATIONSHIP);
    copyAndOverwrite(opt, coreType, AMPLIFICATION_TABLE, Extension.AMPLIFICATION);
    copyAndOverwrite(opt, coreType, CLONING_TABLE, Extension.CLONING);
    copyAndOverwrite(opt, coreType, GEL_IMAGE_TABLE, Extension.GEL_IMAGE);
    copyAndOverwrite(opt, coreType, LOAN_TABLE, Extension.LOAN);
    copyAndOverwrite(opt, coreType, MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE);
    copyAndOverwrite(opt, coreType, PERMIT_TABLE, Extension.PERMIT);
    copyAndOverwrite(opt, coreType, PREPARATION_TABLE, Extension.PREPARATION);
    copyAndOverwrite(opt, coreType, PRESERVATION_TABLE, Extension.PRESERVATION);
    copyAndOverwrite(
        opt, coreType, GERMPLASM_MEASUREMENT_SCORE_TABLE, Extension.GERMPLASM_MEASUREMENT_SCORE);
    copyAndOverwrite(
        opt, coreType, GERMPLASM_MEASUREMENT_TRAIT_TABLE, Extension.GERMPLASM_MEASUREMENT_TRAIT);
    copyAndOverwrite(
        opt, coreType, GERMPLASM_MEASUREMENT_TRIAL_TABLE, Extension.GERMPLASM_MEASUREMENT_TRIAL);
    copyAndOverwrite(opt, coreType, GERMPLASM_ACCESSION_TABLE, Extension.GERMPLASM_ACCESSION);
    copyAndOverwrite(
        opt, coreType, EXTENDED_MEASUREMENT_OR_FACT_TABLE, Extension.EXTENDED_MEASUREMENT_OR_FACT);
    copyAndOverwrite(opt, coreType, CHRONOMETRIC_AGE_TABLE, Extension.CHRONOMETRIC_AGE);
    copyAndOverwrite(opt, coreType, REFERENCE_TABLE, Extension.REFERENCE);
    copyAndOverwrite(opt, coreType, IDENTIFIER_TABLE, Extension.IDENTIFIER);
    copyAndOverwrite(opt, coreType, AUDUBON_TABLE, Extension.AUDUBON);
    copyAndOverwrite(opt, coreType, IMAGE_TABLE, Extension.IMAGE);
    copyAndOverwrite(opt, coreType, MULTIMEDIA_TABLE, Extension.MULTIMEDIA);
    copyAndOverwrite(opt, coreType, DNA_DERIVED_DATA_TABLE, Extension.DNA_DERIVED_DATA);
  }

  private static void copyAll(InterpretationPipelineOptions options) {
    if (options.getCoreRecordType() == OCCURRENCE) {
      copyAndOverwrite(options, OCCURRENCE);
      cleanAndMoveTables(options, OCCURRENCE);
    } else if (options.getCoreRecordType() == EVENT) {
      copyAndOverwrite(options, EVENT);
      cleanAndMoveTables(options, EVENT);
    }
  }

  private static void copyAndOverwrite(
      InterpretationPipelineOptions options, RecordType recordType) {
    String path = recordType.name().toLowerCase();
    copyAndOverwrite(options, recordType, path, path);
  }

  private static <T> void copyAndOverwrite(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Extension extension) {
    String from = extensionRecordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replace("_", "") + "table";
    // clean(options, recordType, extensionRecordType, avroClass);
    copyAndOverwrite(options, recordType, from, to);
  }

  private static void copyAndOverwrite(
      InterpretationPipelineOptions options, RecordType recordType, String from, String to) {
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    // Delete existing avro files in the target directory
    String deletePath =
        PathBuilder.buildPath(
                targetPath, recordType.name().toLowerCase(), to, options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(hdfsConfigs, targetPath, deletePath);

    String filter =
        PathBuilder.buildFilePathViewUsingInputPath(options, recordType, from, "*.avro");

    String copyPath =
        PathBuilder.buildPath(targetPath, recordType.name().toLowerCase(), to).toString();
    log.info("Copying files with pattern {} to {}", filter, copyPath);
    FsUtils.copyToDirectory(hdfsConfigs, copyPath, filter);
    log.info("Files copied to {} directory", copyPath);
  }
}
