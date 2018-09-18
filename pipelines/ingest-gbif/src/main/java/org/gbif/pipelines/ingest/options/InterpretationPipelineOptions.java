package org.gbif.pipelines.ingest.options;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Pipeline options (configuration) for GBIF based data interpretation pipelines. Optionally can use
 * a {@link HadoopFileSystemOptions} when exporting/reading files.
 */
public interface InterpretationPipelineOptions
    extends BasePipelineOptions, HadoopFileSystemOptions {

  @Override
  @Description(
      "Default directory where the target file will be written. By default, it takes the hdfs root directory "
          + "specified in \"fs.defaultFS\". If no configurations are set it takes \"hdfs://\" as default")
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getTargetPath();

  @Override
  void setTargetPath(String targetPath);

  @Description(
      "A HDFS default location for storing temporary files. "
          + "By default uses a tmp directory in the root folder")
  @Default.InstanceFactory(TempDirectoryFactory.class)
  String getHdfsTempLocation();

  void setHdfsTempLocation(String value);

  @Description("Types for an interpretation - ALL, TAXON, LOCATION and etc.")
  List<String> getInterpretationTypes();

  void setInterpretationTypes(List<String> types);

  @Description("WS properties for interpretations that require the use of external web services")
  @JsonIgnore
  String getWsProperties();

  void setWsProperties(String path);

  @Description("Path to hdfs-site-config.xml")
  String getHdfsSiteConfig();

  void setHdfsSiteConfig(String path);

  @Description("Path to core-site-config.xml")
  String getCoreSiteConfig();

  void setCoreSiteConfig(String path);

  /** A {@link DefaultValueFactory} which locates a default directory. */
  class TempDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DefaultDirectoryFactory.getDefaultFs(options)
          .map(fs -> fs + File.separator + "tmp")
          .orElse("hdfs://tmp"); // in case no configurations are provided
    }
  }
}
