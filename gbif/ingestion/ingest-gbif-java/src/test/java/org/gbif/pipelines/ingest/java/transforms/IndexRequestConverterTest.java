package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.index.IndexRequest;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
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
import org.junit.Assert;
import org.junit.Test;

public class IndexRequestConverterTest {

  private static final String ID = "777";

  @Test
  public void converterTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();
    IdentifierRecord idRecord = IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
    BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
    ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
    MetadataRecord metadataRecord = MetadataRecord.newBuilder().setId(ID).build();
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(ID).build();
    TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
    GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
    MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
    ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
    AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();

    // When
    IndexRequest indexRequest =
        IndexRequestConverter.builder()
            .metrics(metrics)
            .metadata(metadataRecord)
            .esIndexName("name")
            .esDocumentId(GBIF_ID)
            .verbatimMap(Map.of(ID, extendedRecord))
            .clusteringMap(Map.of(ID, clusteringRecord))
            .temporalMap(Map.of(ID, temporalRecord))
            .basicMap(Map.of(ID, basicRecord))
            .locationMap(Map.of(ID, locationRecord))
            .taxonMap(Map.of(ID, taxonRecord))
            .grscicollMap(Map.of(ID, grscicollRecord))
            .multimediaMap(Map.of(ID, multimediaRecord))
            .imageMap(Map.of(ID, imageRecord))
            .audubonMap(Map.of(ID, audubonRecord))
            .build()
            .getFn()
            .apply(idRecord);

    // Should
    Assert.assertNotNull(indexRequest);
    Assert.assertEquals("1", indexRequest.id());

    Map<String, Long> map = new HashMap<>();
    metrics
        .getMetricsResult()
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(AVRO_TO_JSON_COUNT));
  }

  @Test
  public void igonreEsDocumentIdConverterTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToEsIndexMetrics();
    IdentifierRecord idRecord = IdentifierRecord.newBuilder().setId(ID).setInternalId("1").build();
    BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
    ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
    MetadataRecord metadataRecord = MetadataRecord.newBuilder().setId(ID).build();
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(ID).build();
    TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
    GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
    MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
    ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
    AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();

    // When
    IndexRequest indexRequest =
        IndexRequestConverter.builder()
            .metrics(metrics)
            .metadata(metadataRecord)
            .esIndexName("name")
            .verbatimMap(Map.of(ID, extendedRecord))
            .temporalMap(Map.of(ID, temporalRecord))
            .clusteringMap(Map.of(ID, clusteringRecord))
            .locationMap(Map.of(ID, locationRecord))
            .basicMap(Map.of(ID, basicRecord))
            .taxonMap(Map.of(ID, taxonRecord))
            .grscicollMap(Map.of(ID, grscicollRecord))
            .multimediaMap(Map.of(ID, multimediaRecord))
            .imageMap(Map.of(ID, imageRecord))
            .audubonMap(Map.of(ID, audubonRecord))
            .build()
            .getFn()
            .apply(idRecord);

    // Should
    Assert.assertNotNull(indexRequest);
    Assert.assertNull(indexRequest.id());

    Map<String, Long> map = new HashMap<>();
    metrics
        .getMetricsResult()
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(AVRO_TO_JSON_COUNT));
  }
}
