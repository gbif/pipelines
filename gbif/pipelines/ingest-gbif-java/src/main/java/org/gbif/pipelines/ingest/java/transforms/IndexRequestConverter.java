package org.gbif.pipelines.ingest.java.transforms;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Builder
public class IndexRequestConverter {

  private final IngestMetrics metrics;
  private final String esDocumentId;

  @NonNull private final String esIndexName;

  @NonNull private final MetadataRecord metadata;
  @NonNull private final Map<String, ExtendedRecord> verbatimMap;
  @NonNull private final Map<String, BasicRecord> basicMap;
  @NonNull private final Map<String, ClusteringRecord> clusteringMap;
  @NonNull private final Map<String, TemporalRecord> temporalMap;
  @NonNull private final Map<String, LocationRecord> locationMap;
  @NonNull private final Map<String, TaxonRecord> taxonMap;
  @NonNull private final Map<String, GrscicollRecord> grscicollMap;
  @NonNull private final Map<String, MultimediaRecord> multimediaMap;
  @NonNull private final Map<String, ImageRecord> imageMap;
  @NonNull private final Map<String, AudubonRecord> audubonMap;

  /** Join all records, convert into string json and IndexRequest for ES */
  public Function<IdentifierRecord, IndexRequest> getFn() {
    return id -> {
      String k = id.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
      ClusteringRecord cr =
          clusteringMap.getOrDefault(k, ClusteringRecord.newBuilder().setId(k).build());
      BasicRecord br = basicMap.getOrDefault(k, BasicRecord.newBuilder().setId(k).build());
      TemporalRecord tr = temporalMap.getOrDefault(k, TemporalRecord.newBuilder().setId(k).build());
      LocationRecord lr = locationMap.getOrDefault(k, LocationRecord.newBuilder().setId(k).build());
      TaxonRecord txr = taxonMap.getOrDefault(k, TaxonRecord.newBuilder().setId(k).build());
      GrscicollRecord gr =
          grscicollMap.getOrDefault(k, GrscicollRecord.newBuilder().setId(k).build());
      // Extension
      MultimediaRecord mr =
          multimediaMap.getOrDefault(k, MultimediaRecord.newBuilder().setId(k).build());
      ImageRecord ir = imageMap.getOrDefault(k, ImageRecord.newBuilder().setId(k).build());
      AudubonRecord ar = audubonMap.getOrDefault(k, AudubonRecord.newBuilder().setId(k).build());

      MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
      OccurrenceJsonRecord json =
          OccurrenceJsonConverter.builder()
              .metadata(metadata)
              .identifier(id)
              .clustering(cr)
              .basic(br)
              .temporal(tr)
              .location(lr)
              .taxon(txr)
              .grscicoll(gr)
              .multimedia(mmr)
              .verbatim(er)
              .build()
              .convert();

      metrics.incMetric(AVRO_TO_JSON_COUNT);

      IndexRequest indexRequest = new IndexRequest(esIndexName).source(json.toString(), JSON);

      // Ignore gbifID as ES doc ID, useful for validator
      if (esDocumentId != null && !esDocumentId.isEmpty()) {
        String docId =
            esDocumentId.equals(GBIF_ID) ? id.getInternalId() : json.get(esDocumentId).toString();
        indexRequest = indexRequest.id(docId);
      }

      return indexRequest;
    };
  }
}
