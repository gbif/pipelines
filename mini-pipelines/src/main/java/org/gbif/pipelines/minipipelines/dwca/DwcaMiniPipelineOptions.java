package org.gbif.pipelines.minipipelines.dwca;

import org.apache.beam.sdk.options.*;

public interface DwcaMiniPipelineOptions extends PipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 1_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 5_242_880L;

  @Description("Path of the input path of the file that will be processed in the pipeline.")
  @Validation.Required
  String getInputPath();

  void setInputPath(String inputPath);

  @Description("Target path where the output will be written to.")
  @Validation.Required
  String getTargetPath();

  void setTargetPath(String targetPath);

  @Description("Id of the dataset.")
  @Validation.Required
  String getDatasetId();

  void setDatasetId(String id);

  @Description("Attempt of the dataset.")
  @Validation.Required
  Integer getAttempt();

  void setAttempt(Integer attempt);

  enum GbifEnv {
    DEV,
    UAT,
    PROD
  }

  @Description(
      "Gbif environment to use when using web services. The existing environments are DEV, UAT and PROD.")
  @Validation.Required
  GbifEnv getGbifEnv();

  void setGbifEnv(GbifEnv env);

  enum PipelineStep {
    DWCA_TO_AVRO, // only reads a Dwca and converts it to an avro file
    INTERPRET, // reads a Dwca and interprets it
    INDEX_TO_ES // reads a Dwca, interprets it and indexes it to ES
  }

  @Description(
      "Steps to run in the pipeline. The possible values are: "
          + "DWCA_TO_AVRO, INTERPRET and INDEX_TO_ES. If no step is specified, INDEX_TO_ES is the default value.")
  @Default.Enum("INDEX_TO_ES")
  PipelineStep getPipelineStep();

  void setPipelineStep(PipelineStep step);

  @Description("If set to true it returns only the output of the last step of the pipeline.")
  @Default.Boolean(false)
  boolean getOmitIntermediateOutputs();

  void setOmitIntermediateOutputs(boolean omitIntermediateOutputs);

  @Description("Target ES Max Batch Size bytes. By default it's set to 5242880.")
  @Default.Long(DEFAULT_ES_BATCH_SIZE_BYTES)
  Long getESMaxBatchSizeBytes();

  void setESMaxBatchSizeBytes(Long batchSize);

  @Description("ES max batch size. By default it's set to 1000.")
  @Default.Long(DEFAULT_ES_BATCH_SIZE)
  long getESMaxBatchSize();

  void setESMaxBatchSize(long esBatchSize);

  @Description("List of ES addresses")
  String[] getESAddresses();

  void setESAddresses(String[] esAddresses);

  @Description(
      "Name of the ES alias. All the indices created will be added to this alias. "
          + "By default we use the alias 'occurrence'.")
  @Default.String("occurrence")
  String getESAlias();

  void setESAlias(String esAlias);

  @Description("Name of the ES index that will be used to index the records.")
  @Hidden
  String getESIndexName();

  void setESIndexName(String esIndexName);


}
