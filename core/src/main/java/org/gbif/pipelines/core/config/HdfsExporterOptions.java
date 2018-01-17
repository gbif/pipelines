package org.gbif.pipelines.core.config;

import java.io.File;
import java.util.List;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * {@link HadoopFileSystemOptions} to use when exporting files to HDFS.
 */
@Experimental(Kind.FILESYSTEM)
public interface HdfsExporterOptions extends HadoopFileSystemOptions {

  @Description("Id of the dataset used to name the target file in HDFS.")
  @Validation.Required
  String getDatasetId();

  void setDatasetId(String id);

  @Description("Directory where the target file will be written. By default, it takes the hdfs root directory "
               + "specified in \"fs.defaultFS\". If no configurations are set it takes \"hdfs://\" as default")
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getTargetDirectory();

  void setTargetDirectory(String targetDirectory);

  @Description("Path of the input file to be copied to HDFS. The path can be absolute "
               + "or relative to the directory where the pipeline is running.")
  @Validation.Required
  String getInputFile();

  void setInputFile(String inputFile);

  @Description("A HDFS default location for storing temporary files. "
               + "By default uses a tmp directory in the root folder")
  @Default.InstanceFactory(TempDirectoryFactory.class)
  String getHdfsTempLocation();

  void setHdfsTempLocation(String value);

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class DefaultDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {

      List<Configuration> configs = options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
      if (configs != null && !configs.isEmpty()) {
        // we take the first config as default
        return configs.get(0).get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
      }

      // return root dir if no configurations are provided
      return "hdfs://";
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TempDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {

      List<Configuration> configs = options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
      if (configs != null && !configs.isEmpty()) {
        // we take the first config as default
        return configs.get(0).get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + File.separator + "tmp";
      }

      // in case no configurations are provided
      return "hdfs://tmp";
    }
  }

}
