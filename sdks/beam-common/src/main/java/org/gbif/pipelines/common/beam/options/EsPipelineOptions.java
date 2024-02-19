package org.gbif.pipelines.common.beam.options;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.GBIF_ID;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Main pipeline options necessary for Elasticsearch index properties */
public interface EsPipelineOptions extends PipelineOptions {

  @Description("Target Elasticsearch Max Batch Size bytes")
  @Default.Long(10_485_760L) // 10mb
  Long getEsMaxBatchSizeBytes();

  void setEsMaxBatchSizeBytes(Long batchSize);

  @Description("Elasticsearch max batch size")
  @Default.Long(1_700L)
  long getEsMaxBatchSize();

  void setEsMaxBatchSize(long esBatchSize);

  @Description("List of Elasticsearch hosts. Required for the DWCA_TO_ES_INDEX step.")
  String[] getEsHosts();

  void setEsHosts(String[] esHosts);

  @Description("Name of the Elasticsearch index that will be used to index the records")
  String getEsIndexName();

  void setEsIndexName(String esIndexName);

  @Description(
      "Name of the Elasticsearch aliases. The index created will be added to this aliases.")
  String[] getEsAlias();

  void setEsAlias(String[] esAlias);

  @Description("Path to an occurrence indexing schema")
  @Default.String("elasticsearch/es-occurrence-schema.json")
  String getEsSchemaPath();

  void setEsSchemaPath(String esSchemaPath);

  @Description("Name of the Elasticsearch user for basic auth")
  String getEsUsername();

  void setEsUsername(String esUsername);

  @Description("Name of the Elasticsearch password for basic auth")
  String getEsPassword();

  void setEsPassword(String esPassword);

  @Description(
      "How often to perform a refresh operation, which makes recent changes to the index visible to search. Defaults to 30s")
  @Default.String("40s")
  String getIndexRefreshInterval();

  void setIndexRefreshInterval(String indexRefreshInterval);

  @Description(
      "The value of this setting determines the number of primary shards in the target index. The default value is 3.")
  @Default.Integer(3)
  Integer getIndexNumberShards();

  void setIndexNumberShards(Integer indexNumberShards);

  @Description(
      "The value of this setting determines the number of replica shards per primary shard in the target index. The default value is 0.")
  @Default.Integer(1)
  Integer getIndexNumberReplicas();

  void setIndexNumberReplicas(Integer indexNumberReplicas);

  @Description("Elasticsearch empty delete index query timeout in seconds")
  @Default.Integer(5)
  Integer getSearchQueryTimeoutSec();

  void setSearchQueryTimeoutSec(Integer searchQueryTimeoutSec);

  @Description("Elasticsearch empty index query attempts")
  @Default.Integer(200)
  Integer getSearchQueryAttempts();

  void setSearchQueryAttempts(Integer searchQueryAttempts);

  @Description("Elasticsearch index max result window")
  @Default.Integer(200_000)
  Integer getIndexMaxResultWindow();

  void setIndexMaxResultWindow(Integer indexMaxResultWindow);

  @Description("Elasticsearch unassigned node delay timeout")
  @Default.String("5m")
  String getUnassignedNodeDelay();

  void setUnassignedNodeDelay(String unassignedNodeDelay);

  @Description("Elasticsearch document id")
  @Default.String(GBIF_ID)
  String getEsDocumentId();

  void setEsDocumentId(String esDocumentId);

  @Description("Limit number of pushing queries at the time")
  @Default.Integer(-1)
  Integer getBackPressure();

  void setBackPressure(Integer backPressure);

  @Description("Use search slowlogs")
  @Default.Boolean(true)
  boolean getUseSlowlog();

  void setUseSlowlog(boolean useSlowlog);

  @Description("Index search slowlog threshold query warn")
  @Default.String("20s")
  String getIndexSearchSlowlogThresholdQueryWarn();

  void setIndexSearchSlowlogThresholdQueryWarn(String indexSearchSlowlogThresholdQueryWarn);

  @Description("Index search slowlog threshold query info")
  @Default.String("10s")
  String getIndexSearchSlowlogThresholdQueryInfo();

  void setIndexSearchSlowlogThresholdQueryInfo(String indexSearchSlowlogThresholdQueryInfo);

  @Description("Index search slowlog threshold fetch warn")
  @Default.String("2s")
  String getIndexSearchSlowlogThresholdFetchWarn();

  void setIndexSearchSlowlogThresholdFetchWarn(String indexSearchSlowlogThresholdFetchWarn);

  @Description("Index search slowlog threshold fetch info")
  @Default.String("1s")
  String getIndexSearchSlowlogThresholdFetchInfo();

  void setIndexSearchSlowlogThresholdFetchInfo(String indexSearchSlowlogThresholdFetchInfo);

  @Description("Index search slowlog level")
  @Default.String("info")
  String getIndexSearchSlowlogLevel();

  void setIndexSearchSlowlogLevel(String indexSearchSlowlogLevel);
}
