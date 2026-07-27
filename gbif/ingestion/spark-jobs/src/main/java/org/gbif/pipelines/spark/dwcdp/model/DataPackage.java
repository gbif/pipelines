package org.gbif.pipelines.spark.dwcdp.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPackage {

  private List<DataPackageResource> resources = new ArrayList<>();

  public Optional<DataPackageResource> findResource(String name) {
    return resources.stream().filter(r -> name.equals(r.getName())).findFirst();
  }
}
