package org.gbif.validator.persistence.mapper;

import org.gbif.validator.api.Metrics;

/** Json TypeHandler for the Metrics class. */
public class MetricsJsonTypeHandler extends JsonTypeHandler<Metrics> {

  public MetricsJsonTypeHandler() {
    super(Metrics.class);
  }
}
