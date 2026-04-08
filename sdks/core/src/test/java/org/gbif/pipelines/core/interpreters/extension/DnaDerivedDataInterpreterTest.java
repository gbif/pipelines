package org.gbif.pipelines.core.interpreters.extension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class DnaDerivedDataInterpreterTest {

  @Test
  public void dnaDerivedDataTest() throws JsonProcessingException {

    // Expected
    String expected =
        "{\"id\":\"id\",\"created\":null,\"dnaDerivedDataItems\":[{\"dnaSequenceID\":\"24ade1fd08fa4338480211bb33fe6202\"},{\"dnaSequenceID\":\"a6bf0d942fd67916f50079e5c44a41b0\"}],\"issues\":{\"issueList\":[]}}";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> dnaDerivedData = new HashMap<>(2);
    dnaDerivedData.put(
        "http://rs.gbif.org/terms/dna_sequence", "ccacacctaaa __  aaactttccacgtgaacc");
    dnaDerivedData.put("https://w3id.org/mixs/0000092", "test");

    Map<String, String> dnaDerivedData2 = new HashMap<>(1);
    dnaDerivedData2.put("http://rs.gbif.org/terms/dna_sequence", "ccacacct");

    ext.put(
        "http://rs.gbif.org/terms/1.0/DNADerivedData", List.of(dnaDerivedData, dnaDerivedData2));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    DnaDerivedDataRecord dr = DnaDerivedDataRecord.newBuilder().setId("id").build();

    // When
    DnaDerivedDataInterpreter.builder().create().interpret(er, dr);

    // Should
    Assert.assertEquals(expected, new ObjectMapper().writeValueAsString(dr));
  }
}
