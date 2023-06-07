package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;

public class StackableConfiguration {

  @Parameter(names = "-kube-config-file")
  public String kubeConfigFile;

  @Parameter(names = "-stackable-spark-crd-file")
  public String sparkCrdConfigFile;

  @Parameter(names = "-delete-pods-on-finish")
  public boolean deletePodsOnFinish;
}
