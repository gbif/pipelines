package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AMPLIFICATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_AGE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CHRONOMETRIC_DATE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CLONING_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GEL_IMAGE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.GERMPLASM_ACCESSION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFICATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOAN_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MATERIAL_SAMPLE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_OR_FACT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_SCORE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_TRAIT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.MEASUREMENT_TRIAL_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PERMIT_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PREPARATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.PRESERVATION_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.REFERENCES_TABLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.RESOURCE_RELATION_TABLE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    move(options, OCCURRENCE_TABLE);
    move(options, MEASUREMENT_OR_FACT_TABLE);
    move(options, IDENTIFICATION_TABLE);
    move(options, RESOURCE_RELATION_TABLE);
    move(options, AMPLIFICATION_TABLE);
    move(options, CLONING_TABLE);
    move(options, GEL_IMAGE_TABLE);
    move(options, LOAN_TABLE);
    move(options, MATERIAL_SAMPLE_TABLE);
    move(options, PERMIT_TABLE);
    move(options, PREPARATION_TABLE);
    move(options, PRESERVATION_TABLE);
    move(options, MEASUREMENT_SCORE_TABLE);
    move(options, MEASUREMENT_TRAIT_TABLE);
    move(options, MEASUREMENT_TRIAL_TABLE);
    move(options, GERMPLASM_ACCESSION_TABLE);
    move(options, EXTENDED_MEASUREMENT_OR_FACT_TABLE);

    move(options, CHRONOMETRIC_AGE_TABLE);
    move(options, CHRONOMETRIC_DATE_TABLE);
    move(options, REFERENCES_TABLE);
    move(options, IDENTIFIER_TABLE);
  }

  private static void move(InterpretationPipelineOptions options, RecordType type) {
    String targetPath = options.getTargetPath();

    String deletePath =
        PathBuilder.buildPath(targetPath, type.getPathName(), options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);

    String filter = PathBuilder.buildFilePathViewUsingInputPath(options, type, "*.avro");

    String movePath = PathBuilder.buildPath(targetPath, type.getPathName()).toString();
    log.info("Moving files with pattern {} to {}", filter, movePath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), movePath, filter);
    log.info("Files moved to {} directory", movePath);
  }
}
