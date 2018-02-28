package org.gbif.pipelines.core.config.option;

import static org.gbif.pipelines.core.config.option.FsTypeEnum.HDFS;
import static org.gbif.pipelines.core.config.option.FsTypeEnum.LOCAL;

public class OptionsFactory {

  private OptionsFactory() {
    // Can't have an instance
  }

  public static Options createOptions(FsTypeEnum fsTypeEnum) {
    if (LOCAL == fsTypeEnum) {
      return new LocalOptions();
    }
    if (HDFS == fsTypeEnum) {
      return new HdfsOptions();
    }
    throw new IllegalArgumentException("Wrong file system type");
  }

}
