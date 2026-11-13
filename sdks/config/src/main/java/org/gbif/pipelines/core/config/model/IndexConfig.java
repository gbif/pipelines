package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexConfig implements Serializable {
  public String refreshInterval;
  public Integer numberReplicas;
  public Integer recordsPerShard;
  public Integer bigIndexIfRecordsMoreThan;
  public String defaultPrefixName = "default";
  public Integer defaultSize;
  public Integer defaultNewIfSize = 23500000;
  public boolean defaultExtraShard = true;
  public String defaultIndexCatUrl = "http://localhost:9200";

  public Integer defaultIndexMinShards = 32;

  // index aliases
  public String occurrenceAlias = "occurrence";
  public String occurrenceVersion;
  public String occurrenceSchemaPath = "elasticsearch/es-occurrence-schema.json";
  public String eventAlias = "event";
  public String eventVersion;
  public String eventSchemaPath = "elasticsearch/es-event-schema.json";

  /**
   * Keeps the _source of the indices. Once records are served from the HBase records tables, set to
   * false: new indices are created with the _source disabled and the fields that aren't indexed
   * aren't sent. Existing indices keep their mappings until they are rebuilt.
   */
  public boolean sourceEnabled = true;
}
