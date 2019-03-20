package org.gbif.pipelines.keygen.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "create")
public class KeygenConfig implements Serializable {

  private static final long serialVersionUID = -8963859065783618024L;

  private OccHbaseConfiguration occHbaseConfiguration;

  private String hbaseZk;
}
