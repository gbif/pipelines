package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Pipeline options necessary for JackKnife */
public interface ClusteringPipelineOptions
    extends PipelineOptions, InterpretationPipelineOptions, AllDatasetsPipelinesOptions {

  @Description("Path to clustering avro files")
  @Default.String("/data/pipelines-clustering")
  String getClusteringPath();

  void setClusteringPath(String clusteringPath);

  @Description(
      "CandidatesCutoff - if we find more than this number of grouped candidates, then drop the cluster")
  @Default.Integer(50)
  Integer getCandidatesCutoff();

  void setCandidatesCutoff(Integer candidatesCutoff);

  @Description("Include sampling")
  @Default.Boolean(false)
  Boolean isOutputDebugAvro();

  void setOutputDebugAvro(Boolean outputDebugAvro);
}
