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
import org.gbif.pipelines.io.avro.extension.ac.AudubonTable;
import org.gbif.pipelines.io.avro.extension.dwc.ChronometricAgeTable;
import org.gbif.pipelines.io.avro.extension.dwc.IdentificationTable;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.extension.dwc.ResourceRelationshipTable;
import org.gbif.pipelines.io.avro.extension.dwc_occurrence.xml.OccurrenceTable;
import org.gbif.pipelines.io.avro.extension.eco.HumboldtTable;
import org.gbif.pipelines.io.avro.extension.gbif.DnaDerivedDataTable;
import org.gbif.pipelines.io.avro.extension.gbif.IdentifierTable;
import org.gbif.pipelines.io.avro.extension.gbif.ImageTable;
import org.gbif.pipelines.io.avro.extension.gbif.MultimediaTable;
import org.gbif.pipelines.io.avro.extension.gbif.ReferenceTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmAccessionTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementScoreTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTraitTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTrialTable;
import org.gbif.pipelines.io.avro.extension.ggbn.AmplificationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.CloningTable;
import org.gbif.pipelines.io.avro.extension.ggbn.GelImageTable;
import org.gbif.pipelines.io.avro.extension.ggbn.LoanTable;
import org.gbif.pipelines.io.avro.extension.ggbn.MaterialSampleTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PermitTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreparationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreservationTable;
import org.gbif.pipelines.io.avro.extension.obis.ExtendedMeasurementOrFactTable;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewAvroUtils {

  /**
   * Cleans empty avro files for extensions and copies all avro files into the directory from
   * targetPath. Deletes pre-existing data of the dataset being processed.
   */
  public static void cleanAndMove(InterpretationPipelineOptions options) {
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

  private static void cleanAndMoveTables(InterpretationPipelineOptions opt, RecordType coreType) {

    cleanMove(
        opt,
        coreType,
        MEASUREMENT_OR_FACT_TABLE,
        Extension.MEASUREMENT_OR_FACT,
        MeasurementOrFactTable.class);
    cleanMove(
        opt, coreType, IDENTIFICATION_TABLE, Extension.IDENTIFICATION, IdentificationTable.class);
    cleanMove(
        opt,
        coreType,
        RESOURCE_RELATIONSHIP_TABLE,
        Extension.RESOURCE_RELATIONSHIP,
        ResourceRelationshipTable.class);
    cleanMove(
        opt, coreType, AMPLIFICATION_TABLE, Extension.AMPLIFICATION, AmplificationTable.class);
    cleanMove(opt, coreType, CLONING_TABLE, Extension.CLONING, CloningTable.class);
    cleanMove(opt, coreType, GEL_IMAGE_TABLE, Extension.GEL_IMAGE, GelImageTable.class);
    cleanMove(opt, coreType, LOAN_TABLE, Extension.LOAN, LoanTable.class);
    cleanMove(
        opt, coreType, MATERIAL_SAMPLE_TABLE, Extension.MATERIAL_SAMPLE, MaterialSampleTable.class);
    cleanMove(opt, coreType, PERMIT_TABLE, Extension.PERMIT, PermitTable.class);
    cleanMove(opt, coreType, PREPARATION_TABLE, Extension.PREPARATION, PreparationTable.class);
    cleanMove(opt, coreType, PRESERVATION_TABLE, Extension.PRESERVATION, PreservationTable.class);
    cleanMove(
        opt,
        coreType,
        GERMPLASM_MEASUREMENT_SCORE_TABLE,
        Extension.GERMPLASM_MEASUREMENT_SCORE,
        GermplasmMeasurementScoreTable.class);
    cleanMove(
        opt,
        coreType,
        GERMPLASM_MEASUREMENT_TRAIT_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRAIT,
        GermplasmMeasurementTraitTable.class);
    cleanMove(
        opt,
        coreType,
        GERMPLASM_MEASUREMENT_TRIAL_TABLE,
        Extension.GERMPLASM_MEASUREMENT_TRIAL,
        GermplasmMeasurementTrialTable.class);
    cleanMove(
        opt,
        coreType,
        GERMPLASM_ACCESSION_TABLE,
        Extension.GERMPLASM_ACCESSION,
        GermplasmAccessionTable.class);
    cleanMove(
        opt,
        coreType,
        EXTENDED_MEASUREMENT_OR_FACT_TABLE,
        Extension.EXTENDED_MEASUREMENT_OR_FACT,
        ExtendedMeasurementOrFactTable.class);
    cleanMove(
        opt,
        coreType,
        CHRONOMETRIC_AGE_TABLE,
        Extension.CHRONOMETRIC_AGE,
        ChronometricAgeTable.class);
    cleanMove(opt, coreType, REFERENCE_TABLE, Extension.REFERENCE, ReferenceTable.class);
    cleanMove(opt, coreType, IDENTIFIER_TABLE, Extension.IDENTIFIER, IdentifierTable.class);
    cleanMove(opt, coreType, AUDUBON_TABLE, Extension.AUDUBON, AudubonTable.class);
    cleanMove(opt, coreType, IMAGE_TABLE, Extension.IMAGE, ImageTable.class);
    cleanMove(opt, coreType, MULTIMEDIA_TABLE, Extension.MULTIMEDIA, MultimediaTable.class);
    cleanMove(
        opt,
        coreType,
        DNA_DERIVED_DATA_TABLE,
        Extension.DNA_DERIVED_DATA,
        DnaDerivedDataTable.class);
    cleanMove(opt, coreType, HUMBOLDT_TABLE, Extension.HUMBOLDT, HumboldtTable.class);
    cleanMove(opt, coreType, OCCURRENCE_TABLE, Extension.OCCURRENCE, OccurrenceTable.class);
  }

  private static void moveAll(InterpretationPipelineOptions options) {
    if (options.getCoreRecordType() == OCCURRENCE) {
      move(options, OCCURRENCE);
      cleanAndMoveTables(options, OCCURRENCE);
    } else if (options.getCoreRecordType() == EVENT) {
      move(options, EVENT);
      cleanAndMoveTables(options, EVENT);
    }
  }

  private static void move(InterpretationPipelineOptions options, RecordType recordType) {
    String path = recordType.name().toLowerCase();
    move(options, recordType, path, path);
  }

  private static <T> void cleanMove(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Extension extension,
      Class<T> avroClass) {
    String from = extensionRecordType.name().toLowerCase();
    String to = extension.name().toLowerCase().replace("_", "") + "table";
    clean(options, recordType, extensionRecordType, avroClass);
    move(options, recordType, from, to);
  }

  private static <T> void clean(
      InterpretationPipelineOptions options,
      RecordType recordType,
      RecordType extensionRecordType,
      Class<T> avroClass) {
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    String extType = extensionRecordType.name().toLowerCase();
    String path = PathBuilder.buildFilePathViewUsingInputPath(options, recordType, extType);
    FsUtils.deleteAvroFileIfEmpty(hdfsConfigs, path, avroClass);
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
