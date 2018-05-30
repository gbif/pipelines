package org.gbif.pipelines.config;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends DataProcessingPipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 1_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 5_242_880L;

  @Description("Target ES Hosts")
  String[] getESHosts();

  void setESHosts(String[] hosts);

  @Description("Target ES Index")
  String getESIndex();

  void setESIndex(String index);

  @Description("Target ES Type")
  String getESType();

  void setESType(String esType);

  @Description("Target ES Max Batch Size")
  @Default.Long(DEFAULT_ES_BATCH_SIZE)
  Long getESMaxBatchSize();

  void setESMaxBatchSize(Long batchSize);

  @Description("Target ES Max Batch Size bytes")
  @Default.Long(DEFAULT_ES_BATCH_SIZE_BYTES)
  Long getESMaxBatchSizeBytes();

  void setESMaxBatchSizeBytes(Long batchSize);
}
