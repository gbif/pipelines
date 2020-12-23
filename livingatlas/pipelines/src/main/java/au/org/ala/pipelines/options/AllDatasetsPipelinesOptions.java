package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.*;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface AllDatasetsPipelinesOptions
    extends PipelineOptions, InterpretationPipelineOptions {

  @Description(
      "Default directory where the target file will be written. By default, it takes the hdfs root directory "
          + "specified in \"fs.defaultFS\". If no configurations are set it takes \"hdfs://\" as default")
  String getAllDatasetsInputPath();

  void setAllDatasetsInputPath(String allDatasetsInputPath);
}
