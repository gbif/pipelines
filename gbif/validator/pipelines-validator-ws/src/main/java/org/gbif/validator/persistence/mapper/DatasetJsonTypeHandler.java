package org.gbif.validator.persistence.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.model.registry.Dataset;

/** Json TypeHandler for the Dataset class. */
public class DatasetJsonTypeHandler extends JsonTypeHandler<Dataset> {

  public DatasetJsonTypeHandler(ObjectMapper mapper) {
    super(mapper, Dataset.class);
  }
}
