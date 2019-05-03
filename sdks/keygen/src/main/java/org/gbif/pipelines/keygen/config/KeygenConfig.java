package org.gbif.pipelines.keygen.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "create")
public class KeygenConfig implements Serializable {

  private static final long serialVersionUID = -8963859065783618024L;

  private String occTable;

  private String counterTable;

  private String lookupTable;

  private String hbaseZk;
}
