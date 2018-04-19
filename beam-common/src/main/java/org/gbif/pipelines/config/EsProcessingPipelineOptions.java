package org.gbif.pipelines.config;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends DataProcessingPipelineOptions {

  int DEFAULT_ES_BATCH_SIZE = 1_000;

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
  @Default.Integer(DEFAULT_ES_BATCH_SIZE)
  Integer getESMaxBatchSize();

  void setESMaxBatchSize(Integer batchSize);
}
