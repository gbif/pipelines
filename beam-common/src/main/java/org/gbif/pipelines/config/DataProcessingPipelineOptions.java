package org.gbif.pipelines.config;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 * Pipeline options (configuration) for GBIF based data pipelines.
 * Optionally can use a {@link HadoopFileSystemOptions} when exporting files.
 */
@Experimental(Kind.FILESYSTEM)
public interface DataProcessingPipelineOptions extends HadoopFileSystemOptions {

  String[] DEFAULT_ES_HOSTS =
    new String[] {"http://c4n1.gbif.org:9200", "http://c4n2.gbif.org:9200", "http://c4n3.gbif.org:9200",
      "http://c4n4.gbif.org:9200", "http://c4n5.gbif.org:9200", "http://c4n6.gbif.org:9200",
      "http://c4n7.gbif.org:9200", "http://c4n8.gbif.org:9200", "http://c4n9.gbif.org:9200"};

  String DEFAULT_ES_INDEX = "interpreted-occurrence";

  String DEFAULT_ES_TYPE = "es-type";

  int DEFAULT_ES_BATCH_SIZE = 1000;

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
  Map<OptionsKeyEnum, TargetPath> getTargetPaths();

  void setTargetPaths(Map<OptionsKeyEnum, TargetPath> targetPaths);

  @Description("Target ES Hosts")
  @Default.InstanceFactory(ESHostsFactory.class)
  String[] getESHosts();

  void setESHosts(String[] hosts);

  @Description("Target ES Index")
  @Default.InstanceFactory(ESIndexFactory.class)
  String getESIndex();

  void setESIndex(String index);

  @Description("Target ES Type")
  @Default.InstanceFactory(ESTypeFactory.class)
  String getESType();

  void setESType(String esType);

  @Description("Target ES Max Batch Size")
  @Default.InstanceFactory(ESMaxBatchSize.class)
  Integer getESMaxBatchSize();

  void setESMaxBatchSize(Integer batchSize);

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class DefaultDirectoryFactory implements DefaultValueFactory<String> {

    private static Optional<String> getHadoopDefaultFs(PipelineOptions options) {
      List<Configuration> configs = options.as(HadoopFileSystemOptions.class).getHdfsConfiguration();
      if (configs != null && !configs.isEmpty()) {
        // we take the first config as default
        return Optional.ofNullable(configs.get(0).get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
      }
      return Optional.empty();
    }

    @Override
    public String create(PipelineOptions options) {
      // return root dir if no configurations are provided
      return getHadoopDefaultFs(options).orElse("hdfs://");
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TempDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DefaultDirectoryFactory.getHadoopDefaultFs(options)
        .map(hadoopFs -> hadoopFs + File.separator + "tmp")
        .orElse("hdfs://tmp"); // in case no configurations are provided
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default directory.
   */
  class TargetPathFactory implements DefaultValueFactory<Map<OptionsKeyEnum, TargetPath>> {
    @Override
    public Map<OptionsKeyEnum, TargetPath> create(PipelineOptions options) {
      String defaultDir = options.as(DataProcessingPipelineOptions.class).getDefaultTargetDirectory();

      return Arrays.stream(OptionsKeyEnum.values())
        .collect(Collectors.toMap(Function.identity(), i -> new TargetPath(defaultDir, i.getDefaultFileName())));

    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search hosts.
   */
  class ESHostsFactory implements DefaultValueFactory<String[]> {

    @Override
    public String[] create(PipelineOptions options) {
      return DEFAULT_ES_HOSTS;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search index.
   */
  class ESIndexFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DEFAULT_ES_INDEX;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search index'es type.
   */
  class ESTypeFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DEFAULT_ES_TYPE;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search type.
   */
  class ESMaxBatchSize implements DefaultValueFactory<Integer> {

    @Override
    public Integer create(PipelineOptions options) {
      return DEFAULT_ES_BATCH_SIZE;
    }
  }

}
