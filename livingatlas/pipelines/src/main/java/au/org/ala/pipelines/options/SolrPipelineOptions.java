package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Main pipeline options necessary for SOLR index for Living atlases */
public interface SolrPipelineOptions extends IndexingPipelineOptions {

  @Description("SOLR collection to index into")
  @Default.String("biocache")
  String getSolrCollection();

  void setSolrCollection(String solrCollection);

  @Description("List of Zookeeper hosts.")
  String getZkHost();

  void setZkHost(String zkHosts);

  @Description("SOLR batch size")
  @Default.Integer(500)
  Integer getSolrBatchSize();

  void setSolrBatchSize(Integer solrBatchSize);

  @Description("SOLR max retry attempts")
  @Default.Integer(10)
  Integer getSolrRetryMaxAttempts();

  void setSolrRetryMaxAttempts(Integer solrRetryMaxAttempts);

  @Description("SOLR max retry attempts")
  @Default.Integer(3)
  Integer getSolrRetryDurationInMins();

  void setSolrRetryDurationInMins(Integer solrRetryDurationInMins);
}
