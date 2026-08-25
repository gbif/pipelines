package org.gbif.validator.ws.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface ValidationMixin {

  @JsonIgnore
  String getClbValidationMessage();
}
