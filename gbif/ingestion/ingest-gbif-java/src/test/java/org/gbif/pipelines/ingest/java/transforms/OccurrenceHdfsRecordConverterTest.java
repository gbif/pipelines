package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.junit.Assert;
import org.junit.Test;

public class OccurrenceHdfsRecordConverterTest {

  private static final String ID = "777";

  @Test
  public void converterTest() {

    // State
    IngestMetrics metrics = IngestMetricsBuilder.createInterpretedToHdfsViewMetrics();
    IdentifierRecord idRecord = IdentifierRecord.newBuilder().setId(ID).setInternalId(ID).build();
    BasicRecord basicRecord = BasicRecord.newBuilder().setId(ID).build();
    ClusteringRecord clusteringRecord = ClusteringRecord.newBuilder().setId(ID).build();
    MetadataRecord metadataRecord = MetadataRecord.newBuilder().setId(ID).build();
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(ID).build();
    TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(ID).build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().setId(ID).build();
    MultiTaxonRecord multiTaxonRecord = MultiTaxonRecord.newBuilder().setId(ID).build();
    GrscicollRecord grscicollRecord = GrscicollRecord.newBuilder().setId(ID).build();
    MultimediaRecord multimediaRecord = MultimediaRecord.newBuilder().setId(ID).build();
    ImageRecord imageRecord = ImageRecord.newBuilder().setId(ID).build();
    DnaDerivedDataRecord dnaDerivedDataRecord = DnaDerivedDataRecord.newBuilder().setId(ID).build();
    AudubonRecord audubonRecord = AudubonRecord.newBuilder().setId(ID).build();
    EventCoreRecord eventCoreRecord = EventCoreRecord.newBuilder().setId(ID).build();

    // When
    List<OccurrenceHdfsRecord> hdfsRecord =
        OccurrenceHdfsRecordConverter.builder()
            .metrics(metrics)
            .metadata(metadataRecord)
            .verbatimMap(Collections.singletonMap(ID, extendedRecord))
            .temporalMap(Collections.singletonMap(ID, temporalRecord))
            .basicMap(Collections.singletonMap(ID, basicRecord))
            .clusteringMap(Collections.singletonMap(ID, clusteringRecord))
            .locationMap(Collections.singletonMap(ID, locationRecord))
            .multiTaxonMap(Collections.singletonMap(ID, multiTaxonRecord))
            .grscicollMap(Collections.singletonMap(ID, grscicollRecord))
            .multimediaMap(Collections.singletonMap(ID, multimediaRecord))
            .imageMap(Collections.singletonMap(ID, imageRecord))
            .dnaMap(Collections.singletonMap(ID, dnaDerivedDataRecord))
            .audubonMap(Collections.singletonMap(ID, audubonRecord))
            .eventCoreRecordMap(Collections.singletonMap(ID, eventCoreRecord))
            .build()
            .getFn()
            .apply(idRecord);

    // Should
    Assert.assertNotNull(hdfsRecord);
    Assert.assertFalse(hdfsRecord.isEmpty());
    Assert.assertEquals(ID, hdfsRecord.get(0).getGbifid());

    Map<String, Long> map = new HashMap<>();
    metrics
        .getMetricsResult()
        .allMetrics()
        .getCounters()
        .forEach(mr -> map.put(mr.getName().getName(), mr.getAttempted()));

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(Long.valueOf(1L), map.get(AVRO_TO_HDFS_COUNT));
  }
}
