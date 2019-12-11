package org.gbif.pipelines.parsers.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/** Models the Contenful access configuration.*/
@Getter
@Data
@AllArgsConstructor(staticName = "create")
public final class ElasticsearchContentConfig implements Serializable {

  // Elasticsearch hosts
  private final String[] hosts;

}

