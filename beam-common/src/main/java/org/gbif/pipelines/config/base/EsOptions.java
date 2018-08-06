package org.gbif.pipelines.config.base;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface EsOptions extends PipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 10_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 10_242_880L; //10mb

  @Description("Target ES Max Batch Size bytes")
  @Default.Long(DEFAULT_ES_BATCH_SIZE_BYTES)
  Long getESMaxBatchSizeBytes();

  void setESMaxBatchSizeBytes(Long batchSize);

  @Description("ES max batch size")
  @Default.Long(DEFAULT_ES_BATCH_SIZE)
  long getESMaxBatchSize();

  void setESMaxBatchSize(long esBatchSize);

  @Description("List of ES hosts. Required for the INDEX_TO_ES step.")
  String[] getESHosts();

  void setESHosts(String[] esHosts);

  @Description("Name of the ES index that will be used to index the records")
  String getESIndexName();

  void setESIndexName(String esIndexName);

  @Description("Name of the ES alias. The index created will be added to this alias.")
  String getESAlias();

  void setESAlias(String esAlias);

  @Description("Path to an occurrence indexing schema")
  @Default.String("elasticsearch/es-occurrence-shcema.json")
  String getESSchemaPath();

  void setESSchemaPath(String esSchemaPath);

  @Description(
      "How often to perform a refresh operation, which makes recent changes to the index visible to search. Defaults to 30s")
  @Default.String("30s")
  String getIndexRefreshInterval();

  void setIndexRefreshInterval(String indexRefreshInterval);

  @Description(
      "The value of this setting determines the number of primary shards in the target index. The default value is 3.")
  @Default.Integer(3)
  Integer getIndexNumberShards();

  void setIndexNumberShards(Integer indexNumberShards);

  @Description(
      "The value of this setting determines the number of replica shards per primary shard in the target index. The default value is 0.")
  @Default.Integer(0)
  Integer getIndexNumberReplicas();

  void setIndexNumberReplicas(Integer indexNumberReplicas);
}
