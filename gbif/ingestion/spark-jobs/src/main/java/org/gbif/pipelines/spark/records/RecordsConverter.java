package org.gbif.pipelines.spark.records;

import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.gbif.api.ws.mixin.Mixins;

/**
 * Converts the JSON documents produced for Elasticsearch into the JSON returned by the API for a
 * single record (interpreted and verbatim views).
 *
 * <p>Implementations reuse the converters of the occurrence project, configured as the web services
 * configure them, so the stored records are identical to the ones the API builds from Elasticsearch
 * hits.
 */
public abstract class RecordsConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /** Same configuration as the occurrence-ws and event-ws ObjectMapper */
  public static final ObjectMapper API_MAPPER = createApiMapper();

  private static final ObjectMapper SOURCE_MAPPER = new ObjectMapper();

  /** API JSON representations of a record, keyed by its HBase row key */
  @Value
  public static class ApiRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    String rowKey;
    String interpreted;
    String verbatim;
  }

  public static RecordsConverter forOccurrences() {
    return new OccurrenceRecordsConverter();
  }

  public static RecordsConverter forEvents() {
    return new EventRecordsConverter();
  }

  /** @param esSourceJson document as indexed in Elasticsearch */
  public ApiRecord convert(String esSourceJson) {
    try {
      Map<String, Object> source = SOURCE_MAPPER.readValue(esSourceJson, MAP_TYPE);
      excludedFields().forEach(path -> removePath(source, path));

      String rowKey = rowKey(source);
      Hit<Map<String, Object>> hit = Hit.of(h -> h.index(indexName()).id(rowKey).source(source));

      return new ApiRecord(
          rowKey,
          API_MAPPER.writeValueAsString(interpreted(hit)),
          API_MAPPER.writeValueAsString(verbatim(hit)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Name of the record key field in the Elasticsearch document */
  public abstract String keyField();

  /**
   * Fields of the indexed documents that aren't part of the API records, they were excluded from
   * the Elasticsearch _source when the API read records from it. Nested fields use dots.
   */
  public abstract List<String> excludedFields();

  protected abstract String indexName();

  protected abstract String rowKey(Map<String, Object> source);

  protected abstract Object interpreted(Hit<Map<String, Object>> hit);

  protected abstract Object verbatim(Hit<Map<String, Object>> hit);

  protected Object requiredKey(Map<String, Object> source) {
    Object key = source.get(keyField());
    if (key == null) {
      throw new IllegalArgumentException("Document without " + keyField());
    }
    return key;
  }

  @SuppressWarnings("unchecked")
  private static void removePath(Map<String, Object> source, String path) {
    int dot = path.indexOf('.');
    if (dot < 0) {
      source.remove(path);
    } else if (source.get(path.substring(0, dot)) instanceof Map) {
      removePath((Map<String, Object>) source.get(path.substring(0, dot)), path.substring(dot + 1));
    }
  }

  private static ObjectMapper createApiMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);
    mapper.disable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    Mixins.getPredefinedMixins().forEach(mapper::addMixIn);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
