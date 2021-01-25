package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Pipeline options necessary for JackKnife */
public interface ClusteringPipelineOptions
    extends PipelineOptions, InterpretationPipelineOptions, AllDatasetsPipelinesOptions {

  @Description("Path to clustering avro files")
  @Default.String("")
  String getClusteringPath();

  void setClusteringPath(String clusteringPath);

  @Description("Whether to dump out candidates for debug")
  @Default.Boolean(false)
  Boolean getDumpCandidatesForDebug();

  void setDumpCandidatesForDebug(Boolean dumpCandidatesForDebug);
}
