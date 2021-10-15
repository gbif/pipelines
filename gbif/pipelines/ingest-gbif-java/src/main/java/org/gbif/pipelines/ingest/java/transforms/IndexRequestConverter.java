package org.gbif.pipelines.ingest.java.transforms;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

@Builder
public class IndexRequestConverter {

  private final IngestMetrics metrics;
  private final String esDocumentId;

  @NonNull private final String esIndexName;

  @NonNull private final MetadataRecord metadata;
  @NonNull private final Map<String, ExtendedRecord> verbatimMap;
  @NonNull private final Map<String, TemporalRecord> temporalMap;
  @NonNull private final Map<String, LocationRecord> locationMap;
  @NonNull private final Map<String, TaxonRecord> taxonMap;
  @NonNull private final Map<String, GrscicollRecord> grscicollMap;
  @NonNull private final Map<String, MultimediaRecord> multimediaMap;
  @NonNull private final Map<String, ImageRecord> imageMap;
  @NonNull private final Map<String, AudubonRecord> audubonMap;

  /** Join all records, convert into string json and IndexRequest for ES */
  public Function<BasicRecord, IndexRequest> getFn() {
    return br -> {
      String k = br.getId();
      // Core
      ExtendedRecord er = verbatimMap.getOrDefault(k, ExtendedRecord.newBuilder().setId(k).build());
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
      ObjectNode json = GbifJsonConverter.toJson(metadata, br, tr, lr, txr, gr, mmr, er);

      metrics.incMetric(AVRO_TO_JSON_COUNT);

      IndexRequest indexRequest = new IndexRequest(esIndexName).source(json.toString(), JSON);

      // Ignore gbifID as ES doc ID, useful for validator
      if (esDocumentId != null && !esDocumentId.isEmpty()) {
        String docId =
            esDocumentId.equals(GBIF_ID)
                ? br.getGbifId().toString()
                : json.get(esDocumentId).asText();
        indexRequest = indexRequest.id(docId);
      }

      return indexRequest;
    };
  }
}
