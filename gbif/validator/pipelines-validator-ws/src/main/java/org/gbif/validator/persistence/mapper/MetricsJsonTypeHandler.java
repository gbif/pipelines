package org.gbif.validator.persistence.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.validator.api.Metrics;

/** Json TypeHandler for the Metrics class. */
public class MetricsJsonTypeHandler extends JsonTypeHandler<Metrics> {

  public MetricsJsonTypeHandler(ObjectMapper mapper) {
    super(mapper, Metrics.class);
  }
}
