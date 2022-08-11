package org.gbif.pipelines.ingest.java.metrics;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AMPLIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CLONING_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTER_ER_BASED_ON_GBIF_ID;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GEL_IMAGE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.GRSCICOLL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_GBIF_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFICATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTIFIER_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.INVALID_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOAN_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MATERIAL_SAMPLE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_SCORE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PERMIT_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PREPARATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.PRESERVATION_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.REFERENCE_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.VERBATIM_RECORDS_COUNT;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.pipelines.VerbatimToOccurrencePipeline;
import org.gbif.pipelines.transforms.common.FilterRecordsTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceJsonTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.table.AmplificationTableTransform;
import org.gbif.pipelines.transforms.table.ChronometricAgeTableTransform;
import org.gbif.pipelines.transforms.table.CloningTableTransform;
import org.gbif.pipelines.transforms.table.ExtendedMeasurementOrFactTableTransform;
import org.gbif.pipelines.transforms.table.GelImageTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmAccessionTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementScoreTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementTraitTableTransform;
import org.gbif.pipelines.transforms.table.GermplasmMeasurementTrialTableTransform;
import org.gbif.pipelines.transforms.table.IdentificationTableTransform;
import org.gbif.pipelines.transforms.table.IdentifierTableTransform;
import org.gbif.pipelines.transforms.table.LoanTableTransform;
import org.gbif.pipelines.transforms.table.MaterialSampleTableTransform;
import org.gbif.pipelines.transforms.table.MeasurementOrFactTableTransform;
import org.gbif.pipelines.transforms.table.OccurrenceHdfsRecordTransform;
import org.gbif.pipelines.transforms.table.PermitTableTransform;
import org.gbif.pipelines.transforms.table.PreparationTableTransform;
import org.gbif.pipelines.transforms.table.PreservationTableTransform;
import org.gbif.pipelines.transforms.table.ReferenceTableTransform;
import org.gbif.pipelines.transforms.table.ResourceRelationshipTableTransform;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IngestMetricsBuilder {

  /** {@link IngestMetrics} for {@link VerbatimToOccurrencePipeline} */
  public static IngestMetrics createVerbatimToInterpretedMetrics() {
    return IngestMetrics.create()
        .addMetric(BasicTransform.class, BASIC_RECORDS_COUNT)
        .addMetric(LocationTransform.class, LOCATION_RECORDS_COUNT)
        .addMetric(MetadataTransform.class, METADATA_RECORDS_COUNT)
        .addMetric(TaxonomyTransform.class, TAXON_RECORDS_COUNT)
        .addMetric(GrscicollTransform.class, GRSCICOLL_RECORDS_COUNT)
        .addMetric(TemporalTransform.class, TEMPORAL_RECORDS_COUNT)
        .addMetric(VerbatimTransform.class, VERBATIM_RECORDS_COUNT)
        .addMetric(AudubonTransform.class, AUDUBON_RECORDS_COUNT)
        .addMetric(ImageTransform.class, IMAGE_RECORDS_COUNT)
        .addMetric(MeasurementOrFactTransform.class, MEASUREMENT_OR_FACT_RECORDS_COUNT)
        .addMetric(MultimediaTransform.class, MULTIMEDIA_RECORDS_COUNT)
        .addMetric(FilterRecordsTransform.class, FILTER_ER_BASED_ON_GBIF_ID)
        .addMetric(UniqueGbifIdTransform.class, UNIQUE_GBIF_IDS_COUNT)
        .addMetric(UniqueGbifIdTransform.class, DUPLICATE_GBIF_IDS_COUNT)
        .addMetric(UniqueGbifIdTransform.class, IDENTICAL_GBIF_OBJECTS_COUNT)
        .addMetric(UniqueGbifIdTransform.class, INVALID_GBIF_ID_COUNT)
        .addMetric(UniqueIdTransform.class, UNIQUE_IDS_COUNT)
        .addMetric(UniqueIdTransform.class, DUPLICATE_IDS_COUNT)
        .addMetric(UniqueIdTransform.class, IDENTICAL_OBJECTS_COUNT)
        .addMetric(OccurrenceExtensionTransform.class, OCCURRENCE_EXT_COUNT);
  }

  /**
   * {@link IngestMetrics} for {@link
   * org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipeline}
   */
  public static IngestMetrics createInterpretedToEsIndexMetrics() {
    return IngestMetrics.create().addMetric(OccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);
  }

  /** {@link IngestMetrics} for hdfs tables */
  public static IngestMetrics createInterpretedToHdfsViewMetrics() {
    return IngestMetrics.create()
        .addMetric(OccurrenceHdfsRecordTransform.class, AVRO_TO_HDFS_COUNT)
        .addMetric(MeasurementOrFactTableTransform.class, MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
        .addMetric(IdentificationTableTransform.class, IDENTIFICATION_TABLE_RECORDS_COUNT)
        .addMetric(
            ResourceRelationshipTableTransform.class, RESOURCE_RELATIONSHIP_TABLE_RECORDS_COUNT)
        .addMetric(AmplificationTableTransform.class, AMPLIFICATION_TABLE_RECORDS_COUNT)
        .addMetric(CloningTableTransform.class, CLONING_TABLE_RECORDS_COUNT)
        .addMetric(GelImageTableTransform.class, GEL_IMAGE_TABLE_RECORDS_COUNT)
        .addMetric(LoanTableTransform.class, LOAN_TABLE_RECORDS_COUNT)
        .addMetric(MaterialSampleTableTransform.class, MATERIAL_SAMPLE_TABLE_RECORDS_COUNT)
        .addMetric(PermitTableTransform.class, PERMIT_TABLE_RECORDS_COUNT)
        .addMetric(PreparationTableTransform.class, PREPARATION_TABLE_RECORDS_COUNT)
        .addMetric(PreservationTableTransform.class, PRESERVATION_TABLE_RECORDS_COUNT)
        .addMetric(
            GermplasmMeasurementScoreTableTransform.class, MEASUREMENT_SCORE_TABLE_RECORDS_COUNT)
        .addMetric(
            GermplasmMeasurementTraitTableTransform.class, MEASUREMENT_TRAIT_TABLE_RECORDS_COUNT)
        .addMetric(
            GermplasmMeasurementTrialTableTransform.class, MEASUREMENT_TRIAL_TABLE_RECORDS_COUNT)
        .addMetric(GermplasmAccessionTableTransform.class, GERMPLASM_ACCESSION_TABLE_RECORDS_COUNT)
        .addMetric(
            ExtendedMeasurementOrFactTableTransform.class,
            EXTENDED_MEASUREMENT_OR_FACT_TABLE_RECORDS_COUNT)
        .addMetric(ChronometricAgeTableTransform.class, CHRONOMETRIC_AGE_TABLE_RECORDS_COUNT)
        .addMetric(ReferenceTableTransform.class, REFERENCE_TABLE_RECORDS_COUNT)
        .addMetric(IdentifierTableTransform.class, IDENTIFIER_TABLE_RECORDS_COUNT);
  }
}
