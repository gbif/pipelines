package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import jakarta.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class ElasticsearchConfiguration {

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
