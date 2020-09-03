package org.gbif.pipelines.ingest.java.metrics;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUDUBON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTER_ER_BASED_ON_GBIF_ID;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_GBIF_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IMAGE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.INVALID_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MEASUREMENT_OR_FACT_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.MULTIMEDIA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TEMPORAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.VERBATIM_RECORDS_COUNT;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.transforms.common.FilterExtendedRecordTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.GbifJsonTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceHdfsRecordConverterTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IngestMetricsBuilder {

  /**
   * {@link IngestMetrics} for {@link
   * org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline}
   */
  public static IngestMetrics createVerbatimToInterpretedMetrics() {
    return IngestMetrics.create()
        .addMetric(BasicTransform.class, BASIC_RECORDS_COUNT)
        .addMetric(LocationTransform.class, LOCATION_RECORDS_COUNT)
        .addMetric(MetadataTransform.class, METADATA_RECORDS_COUNT)
        .addMetric(TaxonomyTransform.class, TAXON_RECORDS_COUNT)
        .addMetric(TemporalTransform.class, TEMPORAL_RECORDS_COUNT)
        .addMetric(VerbatimTransform.class, VERBATIM_RECORDS_COUNT)
        .addMetric(AudubonTransform.class, AUDUBON_RECORDS_COUNT)
        .addMetric(ImageTransform.class, IMAGE_RECORDS_COUNT)
        .addMetric(MeasurementOrFactTransform.class, MEASUREMENT_OR_FACT_RECORDS_COUNT)
        .addMetric(MultimediaTransform.class, MULTIMEDIA_RECORDS_COUNT)
        .addMetric(FilterExtendedRecordTransform.class, FILTER_ER_BASED_ON_GBIF_ID)
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
    return IngestMetrics.create().addMetric(GbifJsonTransform.class, AVRO_TO_JSON_COUNT);
  }

  /**
   * {@link IngestMetrics} for {@link
   * org.gbif.pipelines.ingest.java.pipelines.InterpretedToHdfsViewPipeline}
   */
  public static IngestMetrics createInterpretedToHdfsViewMetrics() {
    return IngestMetrics.create()
        .addMetric(OccurrenceHdfsRecordConverterTransform.class, AVRO_TO_HDFS_COUNT);
  }
}
