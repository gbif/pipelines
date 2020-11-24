package org.gbif.pipelines.core.config.model;

import com.cloudera.org.codehaus.jackson.annotate.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeygenConfig implements Serializable {

  private static final long serialVersionUID = -2392370864481517738L;

  private String occurrenceTable;
  private String counterTable;
  private String lookupTable;
  private String zkConnectionString;
}
