package org.gbif.pipelines.keygen.config;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(buildMethodName = "create")
public class KeygenConfig implements Serializable {

  private static final long serialVersionUID = -8963859065783618025L;

  private String occurrenceTable;
  private String counterTable;
  private String lookupTable;
  private String zkConnectionString;
}
