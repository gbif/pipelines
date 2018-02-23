package org.gbif.pipelines.core.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * {@link HadoopFileSystemOptions} to use when exporting files to HDFS.
 */
@Experimental(Kind.FILESYSTEM)
public interface DataFlowPipelineOptions extends HadoopFileSystemOptions {

  @Description("Id of the dataset used to name the target file in HDFS.")
  @Validation.Required
  String getDatasetId();

  void setDatasetId(String id);

  @Description("Default directory where the target file will be written. By default, it takes the user home directory/gbif-data/{datasetId}")
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getDefaultTargetDirectory();

  void setDefaultTargetDirectory(String targetDirectory);

  @Description("directory where the hadoop related configuration xmls are available, required when writing in HDFS")
  @Default.InstanceFactory(DefaultHDFSConfigurationFactory.class)
  String getHDFSConfigurationDirectory();

  void setHDFSConfigurationDirectory(String configurationDirectory);

  @Description("Path of the input file to be copied to local filesystem/HDFS. The path can be absolute "
               + "or relative to the directory where the pipeline is running.")
  @Validation.Required
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
  Map<RecordInterpretation, String> getTargetPaths();

  void setTargetPaths(Map<RecordInterpretation, String> targetPaths);

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class DefaultDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      String userhome = System.getProperty("user.home");
      return userhome
             + File.separator
             + "gbif-data"
             + File.separator
             + ((DataFlowPipelineOptions) options).getDatasetId();
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class DefaultHDFSConfigurationFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return null;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TempDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      String userhome = System.getProperty("user.home");
      return userhome
             + File.separator
             + "gbif-data"
             + File.separator
             + ((DataFlowPipelineOptions) options).getDatasetId()
             + File.separator
             + "temp";
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TargetPathFactory implements DefaultValueFactory<Map<RecordInterpretation, String>> {

    @Override
    public Map<RecordInterpretation, String> create(PipelineOptions options) {

      Map<RecordInterpretation, String> targetPaths = new HashMap<>();

      String defaultDir = options.as(DataFlowPipelineOptions.class).getDefaultTargetDirectory();

      for (RecordInterpretation interpretation : RecordInterpretation.values()) {
        targetPaths.put(interpretation,
                        TargetPath.fullPath(defaultDir, interpretation.getDefaultFileName() + File.separator + "data"));
      }

      return targetPaths;
    }
  }

}
