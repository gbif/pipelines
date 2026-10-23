package org.gbif.pipelines.spark.records;

import co.elastic.clients.elasticsearch.core.search.Hit;
import java.util.List;
import java.util.Map;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.es.event.SearchHitEventConverter;

/**
 * Converts event documents into the JSON returned by the event API for a single event, using the
 * converter as event-ws configures it. Events are keyed by their internalId.
 */
public class EventRecordsConverter extends RecordsConverter {

  private static final long serialVersionUID = 1L;

  private transient SearchHitEventConverter converter;

  @Override
  public String keyField() {
    return "internalId";
  }

  @Override
  public List<String> excludedFields() {
    return List.of("derivedMetadata.taxonomicCoverage", "all", "index_name", "datasetKey", "count");
  }

  @Override
  protected String indexName() {
    return "event";
  }

  @Override
  protected String rowKey(Map<String, Object> source) {
    return RecordsTableKey.eventRowKey(requiredKey(source).toString());
  }

  @Override
  protected Object interpreted(Hit<Map<String, Object>> hit) {
    return getConverter().apply(hit);
  }

  @Override
  protected Object verbatim(Hit<Map<String, Object>> hit) {
    return getConverter().toVerbatim(hit);
  }

  private SearchHitEventConverter getConverter() {
    if (converter == null) {
      converter = new SearchHitEventConverter(EventEsField.buildFieldMapper(), true);
    }
    return converter;
  }
}
