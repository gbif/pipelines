package org.gbif.pipelines.minipipelines.dwca;

import org.apache.beam.sdk.options.*;

public interface DwcaMiniPipelineOptions extends PipelineOptions {

  long DEFAULT_ES_BATCH_SIZE = 1_000L;
  long DEFAULT_ES_BATCH_SIZE_BYTES = 5_242_880L;

  @Description(
      "Path of the Dwc-A file. It can be a zip file or a folder with the uncompressed files. Required.")
  @Validation.Required
  String getInputPath();

  void setInputPath(String inputPath);

  @Description("Target path where the outputs of the pipeline will be written to. Required.")
  @Validation.Required
  String getTargetPath();

  void setTargetPath(String targetPath);

  @Description("Id of the dataset. Required")
  @Validation.Required
  String getDatasetId();

  void setDatasetId(String id);

  @Description("Attempt of the dataset. Required.")
  @Validation.Required
  Integer getAttempt();

  void setAttempt(Integer attempt);

  enum GbifEnv {
    DEV,
    UAT,
    PROD
  }

  @Description(
      "Gbif environment to use when using web services."
          + "Note that DEV is not accesible from outside GBIF network. Required")
  @Validation.Required
  GbifEnv getGbifEnv();

  void setGbifEnv(GbifEnv env);

  enum PipelineStep {
    DWCA_TO_AVRO, // only reads a Dwca and converts it to an avro file
    INTERPRET, // reads a Dwca and interprets it
    INDEX_TO_ES // reads a Dwca, interprets it and indexes it to ES
  }

  @Description(
      "The pipeline can be configured to run all the steps or only a few of them."
          + "DWCA_TO_AVRO reads a Dwc-A and converts it to an Avro file;"
          + "INTERPRET reads a Dwc-A, interprets it and write the interpreted data and the issues in Avro files;"
          + "INDEX_TO_ES reads a Dwc-A, interprets it and index the interpeted data in a ES index."
          + "All the steps generate an output. If only the final output is desired, "
          + "the intermediate outputs can be ignored by setting the ignoreIntermediateOutputs option to true."
          + " Required.")
  @Default.Enum("INDEX_TO_ES")
  PipelineStep getPipelineStep();

  void setPipelineStep(PipelineStep step);

  @Description("If set to true it returns only the output of the last step of the pipeline.")
  @Default.Boolean(false)
  boolean getIgnoreIntermediateOutputs();

  void setIgnoreIntermediateOutputs(boolean omitIntermediateOutputs);

  @Description("Target ES Max Batch Size bytes.")
  @Default.Long(DEFAULT_ES_BATCH_SIZE_BYTES)
  Long getESMaxBatchSizeBytes();

  void setESMaxBatchSizeBytes(Long batchSize);

  @Description("ES max batch size.")
  @Default.Long(DEFAULT_ES_BATCH_SIZE)
  long getESMaxBatchSize();

  void setESMaxBatchSize(long esBatchSize);

  @Description("List of ES hosts. Required for the INDEX_TO_ES step.")
  String[] getESHosts();

  void setESHosts(String[] esHosts);

  @Description(
      "Name of the ES alias. The index created will be added to this alias. Only applies to the INDEX_TO_ES step.")
  @Default.String("occurrence")
  String getESAlias();

  void setESAlias(String esAlias);

  @Description(
      "Name of the ES index that will be used to index the records. It's for internal use, "
          + "the index will always be set programmatically, so this parameter will be ignored.")
  @Hidden
  String getESIndexName();

  void setESIndexName(String esIndexName);
}
