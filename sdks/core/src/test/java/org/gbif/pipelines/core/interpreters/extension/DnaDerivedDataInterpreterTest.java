package org.gbif.pipelines.core.interpreters.extension;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;

public class DnaDerivedDataInterpreterTest {

  @Test
  public void dnaDerivedDataTest() throws JsonProcessingException {

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> dnaDerivedData = new HashMap<>(2);
    final String seq1 = "ccacacctaaa __  aaactttccacgtgaacc";
    dnaDerivedData.put("http://rs.gbif.org/terms/dna_sequence", seq1);
    dnaDerivedData.put("https://w3id.org/mixs/0000092", "test");

    final String seq2 = "ccacacct";
    Map<String, String> dnaDerivedData2 = new HashMap<>(1);
    dnaDerivedData2.put("http://rs.gbif.org/terms/dna_sequence", seq2);

    ext.put(
        "http://rs.gbif.org/terms/1.0/DNADerivedData", List.of(dnaDerivedData, dnaDerivedData2));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    DnaDerivedDataRecord dr = DnaDerivedDataRecord.newBuilder().setId("id").build();

    // When
    DnaDerivedDataInterpreter.builder().dnaConfig(new DnaConfig()).create().interpret(er, dr);

    // Should
    assertEquals(2, dr.getDnaDerivedDataItems().size());

    DnaDerivedData interpreted1 =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seq1))
            .findFirst()
            .get();
    assertTrue(interpreted1.getInvalid());
    assertTrue(interpreted1.getGapsOrWhitespaceRemoved());

    DnaDerivedData interpreted2 =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seq2))
            .findFirst()
            .get();
    assertFalse(interpreted2.getInvalid());
    assertFalse(interpreted2.getGapsOrWhitespaceRemoved());
  }
}
