package org.gbif.pipelines.spark.records;

import co.elastic.clients.elasticsearch.core.search.Hit;
import java.util.List;
import java.util.Map;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.SearchHitOccurrenceConverter;

/**
 * Converts occurrence documents into the JSON returned by occurrence/{key} and
 * occurrence/{key}/verbatim, using the converter as occurrence-ws configures it.
 */
public class OccurrenceRecordsConverter extends RecordsConverter {

  private static final long serialVersionUID = 1L;

  private transient SearchHitOccurrenceConverter converter;

  @Override
  public String keyField() {
    return "gbifId";
  }

  @Override
  public List<String> excludedFields() {
    return List.of("all");
  }

  @Override
  protected String indexName() {
    return "occurrence";
  }

  @Override
  protected String rowKey(Map<String, Object> source) {
    return RecordsTableKey.occurrenceRowKey(Long.parseLong(requiredKey(source).toString()));
  }

  @Override
  protected Object interpreted(Hit<Map<String, Object>> hit) {
    return getConverter().apply(hit);
  }

  @Override
  protected Object verbatim(Hit<Map<String, Object>> hit) {
    return getConverter().toVerbatimOccurrence(hit);
  }

  private SearchHitOccurrenceConverter getConverter() {
    if (converter == null) {
      converter = new SearchHitOccurrenceConverter(OccurrenceEsField.buildFieldMapper(), true);
    }
    return converter;
  }
}
