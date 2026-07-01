package org.gbif.pipelines.spark.dwcdp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPackageResource {

  private String name;
  private String path;
  private DataPackageSchema schema;
}
