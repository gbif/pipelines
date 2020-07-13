package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

/**
 * Main pipeline options necessary for SOLR index for Living atlases
 */
public interface ALASolrPipelineOptions extends PipelineOptions, InterpretationPipelineOptions {

  @Description("SOLR collection to index into")
  @Default.String("biocache")
  String getSolrCollection();

  void setSolrCollection(String solrCollection);

  @Description("List of Zookeeper hosts.")
  String getZkHost();

  void setZkHost(String zkHosts);

  @Description("Include sampling")
  @Default.Boolean(false)
  Boolean getIncludeSampling();

  void setIncludeSampling(Boolean includeSampling);

  @Description("Include gbif taxonomy")
  @Default.Boolean(false)
  Boolean getIncludeGbifTaxonomy();

  void setIncludeGbifTaxonomy(Boolean includeGbifTaxonomy);

  @Description("SOLR batch size")
  @Default.Integer(500)
  Integer getSolrBatchSize();

  void setSolrBatchSize(Integer solrBatchSize);
}