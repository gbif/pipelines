package org.gbif.pipelines.demo.utils;

import org.gbif.pipelines.core.config.DataFlowPipelineOptions;
import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class PipelineUtils {

  /**
   * Creates a {@link DataProcessingPipelineOptions} from the arguments and configuration passed.
   *
   * @param args   cli args
   * @param config hadoop config
   *
   * @return {@link DataProcessingPipelineOptions}
   */
  public static DataProcessingPipelineOptions createPipelineOptions(Configuration config, String[] args) {
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static DataProcessingPipelineOptions createPipelineOptions(Configuration config) {
    DataProcessingPipelineOptions options = PipelineOptionsFactory.as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  /**
   * create data flow pipeline options from arguments
   */
  public static DataFlowPipelineOptions createPipelineOptions(String[] args) {
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowPipelineOptions.class);
    if (options.getHDFSConfigurationDirectory() != null) {
      options.setHdfsConfiguration(getHadoopConfiguration(options.getHDFSConfigurationDirectory()));
    }
    return options;
  }

  /**
   * gets the configuration from the list of xmls in the hadoop client configuration
   */
  private static List<Configuration> getHadoopConfiguration(String configurationDirectory) {
    Configuration config = new Configuration();
    File file = new File(configurationDirectory);
    if (Files.exists(file.toPath())) {
      //all xml files in one directory
      if (Files.isDirectory(file.toPath())) {
        final List<File> collect = Arrays.asList(file.listFiles())
          .stream()
          .filter((f) -> f.isFile() && f.getName().endsWith(".xml"))
          .collect(Collectors.toList());
        for (File filteredFile : collect) {
          config.addResource(new Path(filteredFile.getAbsolutePath()));
        }
      } else {
        //only one xml file
        config.addResource(new Path(file.getAbsolutePath()));
      }
    }
    return Collections.singletonList(config);
  }

  private PipelineUtils() {}

}
