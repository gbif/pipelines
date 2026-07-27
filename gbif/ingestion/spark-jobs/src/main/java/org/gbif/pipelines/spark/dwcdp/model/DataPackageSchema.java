package org.gbif.pipelines.spark.dwcdp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPackageSchema {

  private List<DataPackageField> fields = new ArrayList<>();
}
