package org.gbif.pipelines.config;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends DataProcessingPipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 1_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 5_242_880L;

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

  @Default.InstanceFactory(DefaultESIndexPrefix.class)
  @Description(
      "ES Index Prefix is a prefix used while creating final index of format <esindexprefix>_datasetid_attempt.")
  String getESIndexPrefix();

  void setESIndexPrefix(String esIndexPrefix);

  /** A {@link DefaultValueFactory} which identifies ES indexPrefix */
  class DefaultESIndexPrefix implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return "interpreted-dataset";
    }
  }

  /** A {@link DefaultValueFactory} which locates ES IPAddresses port */
  class DefaultESAddressFactory implements DefaultValueFactory<String[]> {
    @Override
    public String[] create(PipelineOptions options) {
      return new String[] {
        "http://c3n1.gbif.org:9200", "http://c3n2.gbif.org:9200", "http://c3n3.gbif.org:9200"
      };
    }
  }
}
