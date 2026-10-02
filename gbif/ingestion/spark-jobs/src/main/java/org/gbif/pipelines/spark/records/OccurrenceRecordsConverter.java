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
import java.util.Map;
import lombok.Value;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.ws.mixin.Mixins;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.SearchHitOccurrenceConverter;

/**
 * Converts the occurrence JSON documents produced for Elasticsearch into the JSON returned by the
 * occurrence API for occurrence/{key} and occurrence/{key}/verbatim.
 *
 * <p>The conversion reuses the occurrence-ws converter, configured as occurrence-ws configures it,
 * so the stored records are identical to the ones the API builds from Elasticsearch hits.
 */
public class OccurrenceRecordsConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Field excluded from the Elasticsearch _source, only used for full-text search */
  private static final String FULL_TEXT_FIELD = "all";

  private static final String GBIF_ID_FIELD = "gbifId";

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /** Same configuration as the occurrence-ws ObjectMapper */
  public static final ObjectMapper API_MAPPER = createApiMapper();

  private static final ObjectMapper SOURCE_MAPPER = new ObjectMapper();

  private transient SearchHitOccurrenceConverter converter;

  /** API JSON representations of a record */
  @Value
  public static class OccurrenceRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    long gbifId;
    String interpreted;
    String verbatim;
  }

  /** @param esSourceJson occurrence document as indexed in Elasticsearch */
  public OccurrenceRecord convert(String esSourceJson) {
    try {
      Map<String, Object> source = SOURCE_MAPPER.readValue(esSourceJson, MAP_TYPE);
      source.remove(FULL_TEXT_FIELD);

      Object gbifId = source.get(GBIF_ID_FIELD);
      if (gbifId == null) {
        throw new IllegalArgumentException("Occurrence document without " + GBIF_ID_FIELD);
      }
      long key = Long.parseLong(gbifId.toString());

      Hit<Map<String, Object>> hit =
          Hit.of(h -> h.index("occurrence").id(String.valueOf(key)).source(source));

      Occurrence occurrence = getConverter().apply(hit);
      VerbatimOccurrence verbatim = getConverter().toVerbatimOccurrence(hit);

      return new OccurrenceRecord(
          key, API_MAPPER.writeValueAsString(occurrence), API_MAPPER.writeValueAsString(verbatim));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private SearchHitOccurrenceConverter getConverter() {
    if (converter == null) {
      converter = new SearchHitOccurrenceConverter(OccurrenceEsField.buildFieldMapper(), true);
    }
    return converter;
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
