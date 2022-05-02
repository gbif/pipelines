package org.gbif.pipelines.core.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor(staticName = "create")
@Data
public class HdfsConfigs {
  private final String hdfsSiteConfig;
  private final String coreSiteConfig;
}
