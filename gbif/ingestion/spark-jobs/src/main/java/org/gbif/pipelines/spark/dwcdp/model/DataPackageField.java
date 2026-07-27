package org.gbif.pipelines.spark.dwcdp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPackageField {

  private String name;
  private String type;
}
