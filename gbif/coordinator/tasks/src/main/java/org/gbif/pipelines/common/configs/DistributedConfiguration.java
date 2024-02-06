package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.ToString;

@ToString
public class DistributedConfiguration {

  @Parameter(names = "--distributed-main-class")
  public String mainClass;

  @Parameter(names = "--distributed-jar-path")
  public String jarPath;
}
