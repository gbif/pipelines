package org.gbif.pipelines.spark.records;

import static org.gbif.pipelines.spark.records.RecordsConverter.API_MAPPER;
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
import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.pipelines.spark.records.RecordsConverter.ApiRecord;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.es.event.SearchHitEventConverter;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.SearchHitOccurrenceConverter;
import org.junit.Test;

public class RecordsConverterTest {

  private static final String DATASET_KEY = "7683cc47-cb13-4bad-9614-387c66aa8df0";
  private static final String EVENT_DATASET_KEY = "8d5fe649-f85e-43cc-a19c-2a9979a741ac";
  private static final String EVENT_INTERNAL_ID = "cbf64c0df611eae2fc0c2a3234f0eeac8f423071";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void occurrenceMatchesApiConverter() throws Exception {
    String source = readResource("/records/occurrence-es-source.json");

    ApiRecord record = RecordsConverter.forOccurrences().convert(source);

    // what occurrence-ws builds today from the Elasticsearch hit, _id is the gbifId
    Hit<Map<String, Object>> hit = hit(source, "1", "all");
    SearchHitOccurrenceConverter wsConverter =
        new SearchHitOccurrenceConverter(OccurrenceEsField.buildFieldMapper(), true);

    assertEquals("01:1", record.getRowKey());
    assertEquals(API_MAPPER.writeValueAsString(wsConverter.apply(hit)), record.getInterpreted());
    assertEquals(
        API_MAPPER.writeValueAsString(wsConverter.toVerbatimOccurrence(hit)), record.getVerbatim());
  }

  @Test
  public void eventMatchesApiConverter() throws Exception {
    String source = readResource("/records/event-es-source.json");

    ApiRecord record = RecordsConverter.forEvents().convert(source);

    // what event-ws builds today from the Elasticsearch hit, _id is the internalId
    Hit<Map<String, Object>> hit =
        hit(
            source,
            EVENT_INTERNAL_ID,
            "all",
            "index_name",
            "datasetKey",
            "count",
            "derivedMetadata.taxonomicCoverage");
    SearchHitEventConverter wsConverter =
        new SearchHitEventConverter(EventEsField.buildFieldMapper(), true);

    assertEquals(EVENT_INTERNAL_ID, record.getRowKey());
    assertEquals(API_MAPPER.writeValueAsString(wsConverter.apply(hit)), record.getInterpreted());
    assertEquals(API_MAPPER.writeValueAsString(wsConverter.toVerbatim(hit)), record.getVerbatim());
  }

  @Test
  public void recordsRoundTrip() throws Exception {
    ApiRecord occurrence =
        RecordsConverter.forOccurrences()
            .convert(readResource("/records/occurrence-es-source.json"));
    assertRoundTrip(occurrence.getInterpreted(), Occurrence.class);
    assertRoundTrip(occurrence.getVerbatim(), VerbatimOccurrence.class);

    ApiRecord event =
        RecordsConverter.forEvents().convert(readResource("/records/event-es-source.json"));
    assertRoundTrip(event.getInterpreted(), Event.class);
    assertRoundTrip(event.getVerbatim(), VerbatimOccurrence.class);
  }

  @Test
  public void occurrenceHasApiShape() throws Exception {
    ApiRecord record =
        RecordsConverter.forOccurrences()
            .convert(readResource("/records/occurrence-es-source.json"));

    String interpreted = record.getInterpreted();
    assertEquals(1, (int) JsonPath.read(interpreted, "$.key"));
    assertEquals(DATASET_KEY, JsonPath.read(interpreted, "$.datasetKey"));
    assertEquals("HUMAN_OBSERVATION", JsonPath.read(interpreted, "$.basisOfRecord"));
    assertEquals("KE", JsonPath.read(interpreted, "$.countryCode"));
    assertFalse(JsonPath.<List<?>>read(interpreted, "$.issues").isEmpty());
    assertNotNull(
        JsonPath.read(
            interpreted, "$.extensions['http://rs.tdwg.org/dwc/terms/MeasurementOrFact']"));
    // full-text field is not part of the record
    assertFalse(interpreted.contains("\"all\""));

    String verbatim = record.getVerbatim();
    assertEquals(1, (int) JsonPath.read(verbatim, "$.key"));
    assertEquals("1", JsonPath.read(verbatim, "$['http://rs.gbif.org/terms/1.0/gbifID']"));
    assertTrue(
        JsonPath.<Map<String, Object>>read(verbatim, "$.extensions")
            .containsKey("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"));
  }

  @Test
  public void eventHasApiShape() throws Exception {
    ApiRecord record =
        RecordsConverter.forEvents().convert(readResource("/records/event-es-source.json"));

    assertEquals(EVENT_INTERNAL_ID, JsonPath.read(record.getInterpreted(), "$.id"));
    assertEquals(EVENT_DATASET_KEY, JsonPath.read(record.getInterpreted(), "$.datasetKey"));
    assertEquals("EVT-001", JsonPath.read(record.getInterpreted(), "$.eventID"));
    assertEquals("EVT-000", JsonPath.read(record.getInterpreted(), "$.parentEventID"));
    assertFalse(record.getInterpreted().contains("\"all\""));
    assertEquals(
        "EVT-001",
        JsonPath.read(record.getVerbatim(), "$['http://rs.tdwg.org/dwc/terms/eventID']"));
    assertEquals(
        "EVT-000",
        JsonPath.read(record.getVerbatim(), "$['http://rs.tdwg.org/dwc/terms/parentEventID']"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void occurrenceWithoutKeyIsRejected() {
    RecordsConverter.forOccurrences().convert("{\"datasetKey\":\"" + DATASET_KEY + "\"}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void eventWithoutKeyIsRejected() {
    RecordsConverter.forEvents().convert("{\"type\":\"event\"}");
  }

  @Test
  public void rowKeys() {
    assertEquals("67:1234567", RecordsTableKey.occurrenceRowKey(1234567L));
    assertEquals("02:4000002", RecordsTableKey.occurrenceRowKey(4000002L));
    assertEquals(EVENT_INTERNAL_ID, RecordsTableKey.eventRowKey(EVENT_INTERNAL_ID));
  }

  private static <T> void assertRoundTrip(String json, Class<T> type) throws Exception {
    T value = API_MAPPER.readValue(json, type);
    assertEquals(json, API_MAPPER.writeValueAsString(value));
  }

  /** Builds the hit the web services receive, with _source excludes applied */
  @SuppressWarnings("unchecked")
  private static Hit<Map<String, Object>> hit(String source, String id, String... excludes)
      throws Exception {
    Map<String, Object> map = MAPPER.readValue(source, new TypeReference<>() {});
    for (String exclude : excludes) {
      String[] path = exclude.split("\\.");
      Map<String, Object> parent = map;
      for (int i = 0; i < path.length - 1 && parent != null; i++) {
        parent = (Map<String, Object>) parent.get(path[i]);
      }
      if (parent != null) {
        parent.remove(path[path.length - 1]);
      }
    }
    return Hit.of(h -> h.index("test").id(id).source(map));
  }

  private static String readResource(String path) throws Exception {
    try (InputStream in = RecordsConverterTest.class.getResourceAsStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
