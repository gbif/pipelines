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
  public String defaultIndexCatUrl;

  // index aliases
  public String occurrenceAlias = "occurrence";
  public String occurrenceVersion;
  public String occurrenceSchemaPath = "elasticsearch/es-occurrence-schema.json";
  public String eventAlias = "event";
  public String eventVersion;
  public String eventSchemaPath = "elasticsearch/es-event-schema.json";
}
