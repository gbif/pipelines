package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.ToString;

@ToString
public class DistributedConfiguration {

  @Parameter(names = "--yarn-queue")
  public String yarnQueue;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--distributed-main-class")
  public String mainClass;

  @Parameter(names = "--distributed-jar-path")
  public String jarPath;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--other-user")
  public String otherUser;
}
