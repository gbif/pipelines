package org.gbif.pipelines.spark.util;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.junit.Test;

public class SequenceClassifierUtilsTest {

  @Test
  public void sequenceClassifierUtilsFiltersInvalidSequencesAndMapsCoordinates() throws Exception {
    DnaDerivedData valid =
        DnaDerivedData.newBuilder()
            .setNucleotideSequenceID("seq-1")
            .setSequence("ACGT")
            .setInvalid(false)
            .build();
    DnaDerivedData invalid =
        DnaDerivedData.newBuilder()
            .setNucleotideSequenceID("seq-2")
            .setSequence("NNNN")
            .setInvalid(true)
            .build();
    DnaDerivedData empty = DnaDerivedData.newBuilder().setNucleotideSequenceID("seq-3").build();
    DnaDerivedDataRecord dnaRecord =
        DnaDerivedDataRecord.newBuilder()
            .setId("occ-1")
            .setDnaDerivedDataItems(List.of(valid, invalid, empty))
            .build();

    LocationRecord location =
        LocationRecord.newBuilder().setDecimalLatitude(51.5d).setDecimalLongitude(-1.2d).build();

    Occurrence occurrence =
        Occurrence.builder()
            .id("occ-1")
            .dnaDerivedData(new ObjectMapper().writeValueAsString(dnaRecord))
            .location(new ObjectMapper().writeValueAsString(location))
            .build();

    ClassifyOccurrenceRequest request =
        SequenceClassifierUtils.toClassifyOccurrenceRequest(occurrence);

    assertEquals("occ-1", request.getGbifID());
    assertEquals(51.5d, request.getDecimalLatitude(), 0.00001d);
    assertEquals(-1.2d, request.getDecimalLongitude(), 0.00001d);
    assertEquals(1, request.getNucleotideSequence().size());
    assertEquals("seq-1", request.getNucleotideSequence().get(0).getNucleotideSequenceID());
    assertEquals("ACGT", request.getNucleotideSequence().get(0).getSequence());
    assertEquals(Boolean.FALSE, request.getNucleotideSequence().get(0).getInvalid());
  }
}
