package org.gbif.pipelines.core.config;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public interface DataProcessingPipelineOptions extends HadoopFileSystemOptions {

  @Description("Id of the dataset used to name the target file in HDFS.")
  @Validation.Required
  String getDatasetId();

  void setDatasetId(String id);

  @Description("Default directory where the target file will be written. By default, it takes the hdfs root directory "
               + "specified in \"fs.defaultFS\". If no configurations are set it takes \"hdfs://\" as default")
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getDefaultTargetDirectory();

  void setDefaultTargetDirectory(String targetDirectory);

  @Description("Path of the input file to be copied to HDFS. The path can be absolute "
               + "or relative to the directory where the pipeline is running.")
  String getInputFile();

  void setInputFile(String inputFile);

  @Description("A HDFS default location for storing temporary files. "
               + "By default uses a tmp directory in the root folder")
  @Default.InstanceFactory(TempDirectoryFactory.class)
  String getHdfsTempLocation();

  void setHdfsTempLocation(String value);

  @Description("Target paths for the different data interpretations. If they are not specified, it uses the "
               + "\"DefaultTargetDirectory\" option as directory and the name of the interpretation as file name. "
               + "Interpretations currently supported are verbatim, temporal, location and gbif-backbone.")
  @Default.InstanceFactory(TargetPathFactory.class)
  Map<Interpretation, TargetPath> getTargetPaths();

  void setTargetPaths(Map<Interpretation, TargetPath> targetPaths);

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

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TargetPathFactory implements DefaultValueFactory<Map<Interpretation, TargetPath>> {

    @Override
    public Map<Interpretation, TargetPath> create(PipelineOptions options) {

      Map<Interpretation, TargetPath> targetPaths = new HashMap<>();

      String defaultDir = options.as(DataProcessingPipelineOptions.class).getDefaultTargetDirectory();

      for(Interpretation interpretation : Interpretation.values()) {
        targetPaths.put(interpretation, new TargetPath(defaultDir, interpretation.getDefaultFileName()));
      }

      return targetPaths;
    }
  }

}
