package org.gbif.pipelines.config;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends DataProcessingPipelineOptions{

  String[] DEFAULT_ES_HOSTS =
    new String[] {"http://c4n1.gbif.org:9200", "http://c4n2.gbif.org:9200", "http://c4n3.gbif.org:9200",
      "http://c4n4.gbif.org:9200", "http://c4n5.gbif.org:9200", "http://c4n6.gbif.org:9200",
      "http://c4n7.gbif.org:9200", "http://c4n8.gbif.org:9200", "http://c4n9.gbif.org:9200"};

  String DEFAULT_ES_INDEX = "interpreted-occurrence";

  String DEFAULT_ES_TYPE = "es-type";

  int DEFAULT_ES_BATCH_SIZE = 1_000;

  @Description("Target ES Hosts")
  @Default.InstanceFactory(ESHostsFactory.class)
  String[] getESHosts();

  void setESHosts(String[] hosts);

  @Description("Target ES Index")
  @Default.InstanceFactory(ESIndexFactory.class)
  String getESIndex();

  void setESIndex(String index);

  @Description("Target ES Type")
  @Default.InstanceFactory(ESTypeFactory.class)
  String getESType();

  void setESType(String esType);

  @Description("Target ES Max Batch Size")
  @Default.InstanceFactory(ESMaxBatchSize.class)
  Integer getESMaxBatchSize();

  void setESMaxBatchSize(Integer batchSize);

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search hosts.
   */
  class ESHostsFactory implements DefaultValueFactory<String[]> {

    @Override
    public String[] create(PipelineOptions options) {
      return DEFAULT_ES_HOSTS;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search index.
   */
  class ESIndexFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DEFAULT_ES_INDEX;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search index'es type.
   */
  class ESTypeFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DEFAULT_ES_TYPE;
    }
  }

  /**
   * A {@link DefaultValueFactory} which locates a default elastic search type.
   */
  class ESMaxBatchSize implements DefaultValueFactory<Integer> {

    @Override
    public Integer create(PipelineOptions options) {
      return DEFAULT_ES_BATCH_SIZE;
    }
  }
}
