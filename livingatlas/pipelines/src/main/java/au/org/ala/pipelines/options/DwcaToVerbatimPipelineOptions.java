package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Options for running DwCA to Verbatim AVRO pipelines. */
public interface DwcaToVerbatimPipelineOptions extends InterpretationPipelineOptions {

  @Description("The number of shards to use when writing to HDFS")
  @Default.Integer(48)
  Integer getNumShards();

  void setNumShards(Integer numShards);
}
