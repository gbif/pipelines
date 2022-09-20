package org.gbif.pipelines.core.pojo;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(staticName = "create")
public class HdfsConfigs implements Serializable {

  private static final long serialVersionUID = 2953355237284998443L;

  private final String hdfsSiteConfig;
  private final String coreSiteConfig;

  public static HdfsConfigs nullConfig() {
    return HdfsConfigs.create(null, null);
  }
}
