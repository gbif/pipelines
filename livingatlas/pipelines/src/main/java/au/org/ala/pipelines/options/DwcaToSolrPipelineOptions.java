package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** For ALA sandbox */
public interface DwcaToSolrPipelineOptions extends IndexingPipelineOptions {

  @Default.String("biocache")
  String getSolrCollection();

  void setSolrCollection(String solrCollection);

  @Description("List of ZK hosts")
  String getZkHost();

  void setZkHost(String zkHost);

  @Description("Single SOLR host.")
  String getSolrHost();

  void setSolrHost(String solrHost);

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

  @Description("Include sampling")
  @Default.Boolean(false)
  Boolean getIncludeSampling();

  void setIncludeSampling(Boolean includeSampling);

  @Description("JackKnife path")
  @Default.String("")
  String getJackKnifePath();

  void setJackKnifePath(String jackKnifePath);

  @Description("Include jackknife")
  @Default.Boolean(false)
  Boolean getIncludeJackKnife();

  void setIncludeJackKnife(Boolean includeJackKnife);

  @Description("Include clustering")
  @Default.Boolean(false)
  Boolean getIncludeClustering();

  void setIncludeClustering(Boolean includeClustering);

  @Description("Include distance to expert distribution layers")
  @Default.Boolean(false)
  Boolean getIncludeOutlier();

  void setIncludeOutlier(Boolean includeOutlier);

  @Description("Path to clustering avro files")
  @Default.String("/data/pipelines-clustering")
  String getClusteringPath();

  void setClusteringPath(String clusteringPath);

  @Description("Path to outlier avro files")
  @Default.String("/data/pipelines-outlier")
  String getOutlierPath();

  void setOutlierPath(String outlierPath);

  @Description("Path to outlier avro files")
  @Default.String("/data/pipelines-annotations")
  String getAnnotationsPath();

  void setAnnotationsPath(String annotationsPath);

  @Description("Number of partitions to use")
  @Default.Integer(1)
  Integer getNumOfPartitions();

  void setNumOfPartitions(Integer numOfPartitions);

  @Description("Output AVRO to file path")
  String getOutputAvroToFilePath();

  void setOutputAvroToFilePath(String outputAvroToFilePath);

  @Description("Maximum thread count")
  @Default.Integer(2)
  Integer getMaxThreadCount();

  void setMaxThreadCount(Integer maxThreadCount);
}
