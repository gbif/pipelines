package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContentConfig implements Serializable {

  private static final long serialVersionUID = 6493134179456736118L;

  private int wsTimeoutSec;
  private String[] esHosts;
}
