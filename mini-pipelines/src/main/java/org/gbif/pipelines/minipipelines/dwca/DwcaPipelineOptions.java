package org.gbif.pipelines.minipipelines.dwca;

import org.gbif.pipelines.config.base.BaseOptions;
import org.gbif.pipelines.config.base.EsOptions;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.Validation;

public interface DwcaPipelineOptions extends BaseOptions, EsOptions, SparkPipelineOptions {

  enum GbifEnv {
    DEV,
    UAT,
    PROD
  }

  enum PipelineStep {
    DWCA_TO_AVRO, // only reads a Dwca and converts it to an avro file
    INTERPRET, // reads a Dwca and interprets it
    INDEX_TO_ES // reads a Dwca, interprets it and indexes it to ES
  }

  @Override
  @Description(
      "Path of the Dwc-A file. It can be a zip file or a folder with the uncompressed files. Required.")
  @Validation.Required
  String getInputPath();

  @Override
  void setInputPath(String inputPath);

  @Override
  @Description("Target path where the outputs of the pipeline will be written to. Required.")
  @Validation.Required
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getTargetPath();

  @Override
  void setTargetPath(String targetPath);

  @Description(
      "Gbif environment to use when using web services."
          + "Note that DEV is not accesible from outside GBIF network. Required")
  @Validation.Required
  GbifEnv getGbifEnv();

  void setGbifEnv(GbifEnv env);

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

  @Override
  @Description(
      "Name of the ES index that will be used to index the records. It's for internal use, "
          + "the index will always be set programmatically, so this parameter will be ignored.")
  @Hidden
  String getESIndexName();

  @Override
  void setESIndexName(String esIndexName);

  @Override
  @Description(
      "If set to true it writes the outputs of every step of the pipeline. Otherwise, it writes only the output of the last step.")
  @Default.Boolean(false)
  boolean getWriteOutput();

  @Override
  void setWriteOutput(boolean writeOutput);
}
