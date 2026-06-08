package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContentConfig implements Serializable {

  private static final long serialVersionUID = 6493134179456736118L;

  private int wsTimeoutSec = 60;
  private String[] esHosts;
}
