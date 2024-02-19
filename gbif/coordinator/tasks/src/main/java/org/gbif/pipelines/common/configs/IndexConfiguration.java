package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import javax.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class IndexConfiguration {

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

  @Parameter(names = "--index-default-extra-shard")
  public boolean defaultExtraShard = true;

  @Parameter(names = "--index-default-smallest-index-cat-url")
  @NotNull
  public String defaultSmallestIndexCatUrl;

  @Parameter(names = "--index-occurrence-alias")
  public String occurrenceAlias;

  @Parameter(names = "--index-occurrence-version")
  @NotNull
  public String occurrenceVersion;
}
