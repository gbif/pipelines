package org.gbif.pipelines.ingest.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_AMPLIFICATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_AMPLIFICATION_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CHRONOMETRIC_AGE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CHRONOMETRIC_AGE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CHRONOMETRIC_DATE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CHRONOMETRIC_DATE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CLONING;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_CLONING_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_EXTENDED_MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_EXTENDED_MEASUREMENT_OR_FACT_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_GEL_IMAGE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_GEL_IMAGE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_GERMPLASM_ACCESSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_GERMPLASM_ACCESSION_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_IDENTIFICATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_IDENTIFICATION_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_IDENTIFIER;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_IDENTIFIER_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_LOAN;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_LOAN_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MATERIAL_SAMPLE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MATERIAL_SAMPLE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_OR_FACT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_OR_FACT_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_SCORE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_SCORE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_TRAIT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_TRAIT_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_TRIAL;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_MEASUREMENT_TRIAL_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_OCCURRENCE_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PERMIT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PERMIT_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PREPARATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PREPARATION_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PRESERVATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_PRESERVATION_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_REFERENCES;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_REFERENCES_DIR;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_RESOURCE_RELATION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView.VIEW_RESOURCE_RELATION_DIR;
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
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE_HDFS_RECORD;
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

    move(options, VIEW_OCCURRENCE_DIR, VIEW_OCCURRENCE, OCCURRENCE_HDFS_RECORD);
    move(
        options, VIEW_MEASUREMENT_OR_FACT_DIR, VIEW_MEASUREMENT_OR_FACT, MEASUREMENT_OR_FACT_TABLE);
    move(options, VIEW_IDENTIFICATION_DIR, VIEW_IDENTIFICATION, IDENTIFICATION_TABLE);
    move(options, VIEW_RESOURCE_RELATION_DIR, VIEW_RESOURCE_RELATION, RESOURCE_RELATION_TABLE);
    move(options, VIEW_AMPLIFICATION_DIR, VIEW_AMPLIFICATION, AMPLIFICATION_TABLE);
    move(options, VIEW_CLONING_DIR, VIEW_CLONING, CLONING_TABLE);
    move(options, VIEW_GEL_IMAGE_DIR, VIEW_GEL_IMAGE, GEL_IMAGE_TABLE);
    move(options, VIEW_LOAN_DIR, VIEW_LOAN, LOAN_TABLE);
    move(options, VIEW_MATERIAL_SAMPLE_DIR, VIEW_MATERIAL_SAMPLE, MATERIAL_SAMPLE_TABLE);
    move(options, VIEW_PERMIT_DIR, VIEW_PERMIT, PERMIT_TABLE);
    move(options, VIEW_PREPARATION_DIR, VIEW_PREPARATION, PREPARATION_TABLE);
    move(options, VIEW_PRESERVATION_DIR, VIEW_PRESERVATION, PRESERVATION_TABLE);
    move(options, VIEW_MEASUREMENT_SCORE_DIR, VIEW_MEASUREMENT_SCORE, MEASUREMENT_SCORE_TABLE);
    move(options, VIEW_MEASUREMENT_TRAIT_DIR, VIEW_MEASUREMENT_TRAIT, MEASUREMENT_TRAIT_TABLE);
    move(options, VIEW_MEASUREMENT_TRIAL_DIR, VIEW_MEASUREMENT_TRIAL, MEASUREMENT_TRIAL_TABLE);
    move(
        options, VIEW_GERMPLASM_ACCESSION_DIR, VIEW_GERMPLASM_ACCESSION, GERMPLASM_ACCESSION_TABLE);
    move(
        options,
        VIEW_EXTENDED_MEASUREMENT_OR_FACT_DIR,
        VIEW_EXTENDED_MEASUREMENT_OR_FACT,
        EXTENDED_MEASUREMENT_OR_FACT_TABLE);
    move(options, VIEW_CHRONOMETRIC_AGE_DIR, VIEW_CHRONOMETRIC_AGE, CHRONOMETRIC_AGE_TABLE);
    move(options, VIEW_CHRONOMETRIC_DATE_DIR, VIEW_CHRONOMETRIC_DATE, CHRONOMETRIC_DATE_TABLE);
    move(options, VIEW_REFERENCES_DIR, VIEW_REFERENCES, REFERENCES_TABLE);
    move(options, VIEW_IDENTIFIER_DIR, VIEW_IDENTIFIER, IDENTIFIER_TABLE);
  }

  private static void move(
      InterpretationPipelineOptions options, String viewDir, String view, RecordType type) {
    String targetPath = options.getTargetPath();

    String deletePath =
        PathBuilder.buildPath(targetPath, viewDir, view + "_" + options.getDatasetId() + "_*")
            .toString();
    log.info("Deleting avro files {}", deletePath);
    FsUtils.deleteByPattern(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), targetPath, deletePath);

    String filter = PathBuilder.buildFilePathViewUsingInputPath(options, type, view, "*.avro");

    String movePath = PathBuilder.buildPath(targetPath, viewDir).toString();
    log.info("Moving files with pattern {} to {}", filter, movePath);
    FsUtils.moveDirectory(
        options.getHdfsSiteConfig(), options.getCoreSiteConfig(), movePath, filter);
    log.info("Files moved to {} directory", movePath);
  }
}
