package org.gbif.pipelines.config;

import java.io.File;
import java.util.Collections;
import java.util.EnumMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class DataPipelineOptionsFactory {

  private DataPipelineOptionsFactory() {
    // Can't have an instance
  }

  /**
   * Creates a {@link DataProcessingPipelineOptions} from the arguments and configuration passed.
   *
   * @param args   cli args
   * @param config hadoop config
   *
   * @return {@link DataProcessingPipelineOptions}
   */
  public static DataProcessingPipelineOptions create(Configuration config, String[] args) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static DataProcessingPipelineOptions create(String[] args) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataProcessingPipelineOptions.class);
    loadHadoopConfigFromPath(options);

    return options;
  }

  public static DataProcessingPipelineOptions create(Configuration config) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options = PipelineOptionsFactory.as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  private static DataProcessingPipelineOptions createPipelineOptionsFromArgsWithoutValidation(
    Configuration config, String[] args
  ) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  /**
   * Creates a PipelineOptions suitable to interpret taxonomic records in HDFS.
   */
  @VisibleForTesting
  public static DataProcessingPipelineOptions createDefaultTaxonOptions(
    Configuration config, String sourcePath, String taxonOutPath, String issuesOutPath, String[] args
  ) {
    // create options
    DataProcessingPipelineOptions options = createPipelineOptionsFromArgsWithoutValidation(config, args);
    options.setInputFile(sourcePath);

    // target paths
    EnumMap<OptionsKeyEnum, TargetPath> targetPaths = new EnumMap<>(OptionsKeyEnum.class);
    targetPaths.put(OptionsKeyEnum.GBIF_BACKBONE,
                    new TargetPath(taxonOutPath, OptionsKeyEnum.GBIF_BACKBONE.getDefaultFileName()));
    targetPaths.put(OptionsKeyEnum.ISSUES, new TargetPath(issuesOutPath, OptionsKeyEnum.ISSUES.getDefaultFileName()));
    options.setTargetPaths(targetPaths);

    return options;
  }

  public static EsProcessingPipelineOptions createForEs(String[] args) {
    PipelineOptionsFactory.register(EsProcessingPipelineOptions.class);
    EsProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(EsProcessingPipelineOptions.class);
    loadHadoopConfigFromPath(options);

    return options;
  }

  private static void loadHadoopConfigFromPath(DataProcessingPipelineOptions options) {
    String hdfsPath = options.getHdfsSiteConfig();
    String corePath = options.getCoreSiteConfig();
    boolean isHdfsExist = !Strings.isNullOrEmpty(hdfsPath) && new File(hdfsPath).exists();
    boolean isCoreExist = !Strings.isNullOrEmpty(corePath) && new File(corePath).exists();
    if (isHdfsExist && isCoreExist) {
      Configuration conf = new Configuration(false);
      conf.addResource(new Path(hdfsPath));
      conf.addResource(new Path(corePath));
      options.setHdfsConfiguration(Collections.singletonList(conf));
    }
  }

}
