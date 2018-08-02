package org.gbif.pipelines.config;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends DataProcessingPipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 20_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 20_971_520L; // 20mb

  @Description("Target ES Max Batch Size bytes")
  @Default.Long(DEFAULT_ES_BATCH_SIZE_BYTES)
  Long getESMaxBatchSizeBytes();

  void setESMaxBatchSizeBytes(Long batchSize);

  @Description("ES max batch size")
  @Default.Long(DEFAULT_ES_BATCH_SIZE)
  long getESMaxBatchSize();

  void setESMaxBatchSize(long esBatchSize);

  @Default.InstanceFactory(DefaultESAddressFactory.class)
  @Description(
      "List of ES addresses eg. \"http://c3n1.gbif.org:9200\", \"http://c3n2.gbif.org:9200\", \"http://c3n3.gbif.org:9200\"")
  String[] getESAddresses();

  void setESAddresses(String[] esAddresses);

  /** A {@link DefaultValueFactory} which locates ES IPAddresses port */
  class DefaultESAddressFactory implements DefaultValueFactory<String[]> {
    @Override
    public String[] create(PipelineOptions options) {
      return new String[] {
        "http://c3n1.gbif.org:9200", "http://c3n2.gbif.org:9200", "http://c3n3.gbif.org:9200"
      };
    }
  }

  @Description(
      "ES Index Prefix is a prefix used while creating final index of format <esindexprefix>_datasetid_attempt.")
  @Default.String("interpreted-dataset")
  String getESIndexPrefix();

  void setESIndexPrefix(String esIndexPrefix);

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
