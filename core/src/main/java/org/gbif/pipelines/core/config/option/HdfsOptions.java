package org.gbif.pipelines.core.config.option;

import java.io.File;
import java.util.List;
import java.util.Optional;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

public class HdfsOptions implements Options {

  @Override
  public String createDefaultDirectoryFactory(PipelineOptions options) {
    // return root dir if no configurations are provided
    return getHadoopDefaultFs(options).orElse("hdfs://");
  }

  @Override
  public String createTempDirectoryFactory(PipelineOptions options) {
    return getHadoopDefaultFs(options).map(hadoopFs -> hadoopFs + File.separator + "tmp")
      .orElse("hdfs://tmp"); // in case no configurations are provided
  }

  private static Optional<String> getHadoopDefaultFs(PipelineOptions options) {
    List<Configuration> configs = options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
    if (configs != null && !configs.isEmpty()) {
      // we take the first config as default
      return Optional.ofNullable(configs.get(0).get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    }
    return Optional.empty();
  }

}
