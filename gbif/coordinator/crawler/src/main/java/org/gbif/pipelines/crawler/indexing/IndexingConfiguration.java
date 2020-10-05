package org.gbif.pipelines.crawler.indexing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.*;

/** Configuration required to start Indexing Pipeline on provided dataset */
@ToString
public class IndexingConfiguration implements BaseConfiguration {

  @ParametersDelegate @Valid @NotNull public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate @Valid @NotNull
  public ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration();

  @ParametersDelegate @Valid @NotNull
  public IndexConfiguration indexConfig = new IndexConfiguration();

  @ParametersDelegate @Valid public SparkConfiguration sparkConfig = new SparkConfiguration();

  @ParametersDelegate @Valid
  public DistributedConfiguration distributedConfig = new DistributedConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.INTERPRETED_TO_INDEX + ".yml";

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Override
  public String getHdfsSiteConfig() {
    return stepConfig.hdfsSiteConfig;
  }

  @Override
  public String getCoreSiteConfig() {
    return stepConfig.coreSiteConfig;
  }

  @Override
  public String getRepositoryPath() {
    return stepConfig.repositoryPath;
  }

  @Override
  public String getMetaFileName() {
    return metaFileName;
  }

  @ToString
  public static class ElasticsearchConfiguration {

    @Parameter(names = "--es-max-batch-size-bytes")
    public Long maxBatchSizeBytes;

    @Parameter(names = "--es-max-batch-size")
    public Long maxBatchSize;

    @Parameter(names = "--es-hosts")
    @NotNull
    public String[] hosts;

    @Parameter(names = "--es-schema-path")
    public String schemaPath;
  }

  @ToString
  public static class IndexConfiguration {

    @Parameter(names = "--index-refresh-interval")
    public String refreshInterval;

    @Parameter(names = "--index-number-replicas")
    public Integer numberReplicas;

    @Parameter(names = "--index-records-per-shard")
    @NotNull
    public Integer recordsPerShard;

    @Parameter(names = "--index-big-index-if-records-more-than")
    @NotNull
    public Integer bigIndexIfRecordsMoreThan;

    @Parameter(names = "--index-default-prefix-name")
    @NotNull
    public String defaultPrefixName;

    @Parameter(names = "--index-default-size")
    @NotNull
    public Integer defaultSize;

    @Parameter(names = "--index-default-new-if-size")
    @NotNull
    public Integer defaultNewIfSize;

    @Parameter(names = "--index-default-smallest-index-cat-url")
    @NotNull
    public String defaultSmallestIndexCatUrl;

    @Parameter(names = "--index-occurrence-alias")
    public String occurrenceAlias;

    @Parameter(names = "--index-occurrence-version")
    @NotNull
    public String occurrenceVersion;
  }
}
