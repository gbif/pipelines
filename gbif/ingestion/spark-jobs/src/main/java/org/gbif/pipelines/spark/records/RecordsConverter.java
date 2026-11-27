package org.gbif.pipelines.spark.records;

import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.ws.mixin.Mixins;
import org.gbif.api.ws.mixin.OccurrenceMixin;

/**
 * Converts the JSON documents produced for Elasticsearch into the JSON returned by the API for a
 * single record (interpreted and verbatim views).
 *
 * <p>Implementations reuse the converters of the occurrence project, configured as the web services
 * configure them, so the stored records are identical to the ones the API builds from Elasticsearch
 * hits.
 */
public abstract class RecordsConverter implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /** Same configuration as the occurrence-ws and event-ws ObjectMapper */
  public static final ObjectMapper API_MAPPER = createApiMapper();

  private static final ObjectMapper SOURCE_MAPPER = new ObjectMapper();

  /** API JSON representations of a record, keyed by its HBase row key */
  public record ApiRecord(String rowKey, String interpreted, String verbatim)
      implements Serializable {

    @Serial private static final long serialVersionUID = 1L;
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
      return convert(SOURCE_MAPPER.readValue(esSourceJson, MAP_TYPE));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param source document as indexed in Elasticsearch, as Jackson reads it into a map. It is
   *     modified, the excluded fields are removed from it.
   */
  public ApiRecord convert(Map<String, Object> source) {
    try {
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
    mapper.addMixIn(Occurrence.class, ThreadSafeOccurrenceMixin.class);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }

  /**
   * {@link OccurrenceMixin} serializes dateIdentified with a static SimpleDateFormat
   * (DateSerde.NoTimezoneDateJsonSerializer), which isn't thread-safe and fails when several Spark
   * tasks share an executor. This keeps the same mixin and output, using a thread-safe formatter.
   */
  interface ThreadSafeOccurrenceMixin extends OccurrenceMixin {

    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonSerialize(using = NoTimezoneDateSerializer.class)
    Date getDateIdentified();
  }

  /** Same format as DateSerde.NoTimezoneDateJsonSerializer */
  static class NoTimezoneDateSerializer extends JsonSerializer<Date> {

    private static final DateTimeFormatter NO_TIMEZONE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    @Override
    public void serialize(Date value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (value != null) {
        gen.writeString(NO_TIMEZONE_FORMAT.format(value.toInstant()));
      }
    }
  }
}
