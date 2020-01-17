package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import lombok.Data;

/** Models the Contenful access configuration. */
@Data(staticConstructor = "create")
public final class ElasticsearchContentConfig implements Serializable {

  private static final long serialVersionUID = -9119714539999567271L;

  // Elasticsearch hosts
  private final String[] hosts;

}

