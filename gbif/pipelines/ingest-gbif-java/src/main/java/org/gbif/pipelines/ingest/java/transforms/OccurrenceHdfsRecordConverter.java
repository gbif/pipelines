package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

@Builder
public class OccurrenceHdfsRecordConverter {

  private final IngestMetrics metrics;

  @NonNull private final MetadataRecord metadata;
  @NonNull private final Map<String, ExtendedRecord> verbatimMap;
  private Map<String, ClusteringRecord> clusteringMap;
  private Map<String, BasicRecord> basicMap;
  @NonNull private final Map<String, TemporalRecord> temporalMap;
  @NonNull private final Map<String, LocationRecord> locationMap;
  @NonNull private final Map<String, TaxonRecord> taxonMap;
  private Map<String, GrscicollRecord> grscicollMap;
  @NonNull private final Map<String, MultimediaRecord> multimediaMap;
  @NonNull private final Map<String, ImageRecord> imageMap;
  @NonNull private final Map<String, AudubonRecord> audubonMap;
  private Map<String, EventCoreRecord> eventCoreRecordMap;

  /** Join all records, convert into OccurrenceHdfsRecord and save as an avro file */
  public Function<GbifIdRecord, Optional<OccurrenceHdfsRecord>> getFn() {
    return id -> {
      String k = id.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
      TemporalRecord tr = temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
      LocationRecord lr = locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
      TaxonRecord txr = taxonMap.getOrDefault(k, TaxonRecord.newBuilder().setId(k).build());

      // Extension
      MultimediaRecord mr =
          multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
      ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
      AudubonRecord ar = audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());

      MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

      metrics.incMetric(AVRO_TO_HDFS_COUNT);

      org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter
              .OccurrenceHdfsRecordConverterBuilder
          hdfsRecord =
              org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter.builder()
                  .gbifIdRecord(id)
                  .metadataRecord(metadata)
                  .temporalRecord(tr)
                  .locationRecord(lr)
                  .taxonRecord(txr)
                  .multimediaRecord(mmr)
                  .extendedRecord(er);

      Optional.ofNullable(basicMap)
          .ifPresent(
              bm ->
                  hdfsRecord.basicRecord(
                      bm.getOrDefault(k, BasicRecord.newBuilder().setId(k).build())));

      Optional.ofNullable(clusteringMap)
          .ifPresent(
              cm ->
                  hdfsRecord.clusteringRecord(
                      cm.getOrDefault(k, ClusteringRecord.newBuilder().setId(k).build())));

      Optional.ofNullable(grscicollMap)
          .ifPresent(
              gm ->
                  hdfsRecord.grscicollRecord(
                      gm.getOrDefault(k, GrscicollRecord.newBuilder().setId(k).build())));

      Optional.ofNullable(eventCoreRecordMap)
          .ifPresent(
              em ->
                  hdfsRecord.eventCoreRecord(
                      em.getOrDefault(k, EventCoreRecord.newBuilder().setId(k).build())));

      return Optional.of(hdfsRecord.build().convert());
    };
  }
}
