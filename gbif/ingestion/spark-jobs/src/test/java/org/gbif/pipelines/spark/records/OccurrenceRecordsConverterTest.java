package org.gbif.pipelines.spark.records;

import static org.gbif.pipelines.spark.records.OccurrenceRecordsConverter.API_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.pipelines.spark.records.OccurrenceRecordsConverter.OccurrenceRecord;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.SearchHitOccurrenceConverter;
import org.junit.Test;

public class OccurrenceRecordsConverterTest {

  private static final String DATASET_KEY = "7683cc47-cb13-4bad-9614-387c66aa8df0";

  private final OccurrenceRecordsConverter converter = new OccurrenceRecordsConverter();

  @Test
  public void convertMatchesApiConverter() throws Exception {
    String source = readFixture();

    OccurrenceRecord record = converter.convert(source);

    // what occurrence-ws builds today from the Elasticsearch hit
    Map<String, Object> sourceMap =
        new ObjectMapper().readValue(source, new TypeReference<Map<String, Object>>() {});
    Hit<Map<String, Object>> hit = Hit.of(h -> h.index("occurrence").id("1").source(sourceMap));
    SearchHitOccurrenceConverter wsConverter =
        new SearchHitOccurrenceConverter(OccurrenceEsField.buildFieldMapper(), true);

    assertEquals(1L, record.getGbifId());
    assertEquals(API_MAPPER.writeValueAsString(wsConverter.apply(hit)), record.getInterpreted());
    assertEquals(
        API_MAPPER.writeValueAsString(wsConverter.toVerbatimOccurrence(hit)), record.getVerbatim());
  }

  @Test
  public void interpretedRoundTrips() throws Exception {
    OccurrenceRecord record = converter.convert(readFixture());

    Occurrence occurrence = API_MAPPER.readValue(record.getInterpreted(), Occurrence.class);
    assertEquals(record.getInterpreted(), API_MAPPER.writeValueAsString(occurrence));

    VerbatimOccurrence verbatim =
        API_MAPPER.readValue(record.getVerbatim(), VerbatimOccurrence.class);
    assertEquals(record.getVerbatim(), API_MAPPER.writeValueAsString(verbatim));
  }

  @Test
  public void interpretedHasApiShape() throws Exception {
    String json = converter.convert(readFixture()).getInterpreted();

    assertEquals(1, (int) JsonPath.read(json, "$.key"));
    assertEquals(DATASET_KEY, JsonPath.read(json, "$.datasetKey"));
    assertEquals("HUMAN_OBSERVATION", JsonPath.read(json, "$.basisOfRecord"));
    assertEquals("KE", JsonPath.read(json, "$.countryCode"));
    assertFalse(JsonPath.<List<?>>read(json, "$.issues").isEmpty());
    assertNotNull(
        JsonPath.read(json, "$.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact']"));
    // full-text field is not part of the record
    assertFalse(json.contains("\"all\""));
  }

  @Test
  public void verbatimHasApiShape() throws Exception {
    String json = converter.convert(readFixture()).getVerbatim();

    assertEquals(1, (int) JsonPath.read(json, "$.key"));
    assertEquals(DATASET_KEY, JsonPath.read(json, "$.datasetKey"));
    assertEquals("1", JsonPath.read(json, "$['http://rs.gbif.org/terms/1.0/gbifID']"));
    assertTrue(
        JsonPath.<Map<String, Object>>read(json, "$.extensions")
            .containsKey("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void documentWithoutGbifIdIsRejected() {
    converter.convert("{\"datasetKey\":\"" + DATASET_KEY + "\"}");
  }

  @Test
  public void rowKeyIsSaltedWithKeyModulo() {
    assertEquals("67:1234567", RecordsTableKey.rowKey(1234567L));
    assertEquals("02:4000002", RecordsTableKey.rowKey(4000002L));
    assertEquals(67, RecordsTableKey.salt("67:1234567"));
    assertEquals(2, RecordsTableKey.salt("02:4000002"));
  }

  private String readFixture() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/records/occurrence-es-source.json")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
