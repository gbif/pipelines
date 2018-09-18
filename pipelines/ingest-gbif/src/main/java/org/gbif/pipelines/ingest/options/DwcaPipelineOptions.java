package org.gbif.pipelines.ingest.options;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.Validation;

public interface DwcaPipelineOptions extends EsIndexingPipelineOptions, SparkPipelineOptions {

  enum PipelineStep {
    DWCA_TO_AVRO, // only reads a Dwca and converts it to an avro file
    INTERPRET, // reads a Dwca and interprets it
    INDEX_TO_ES, // reads a Dwca, interprets it and indexes it to ES
    INTERPRET_TO_INDEX // reads interpreted avro files and indexes them to ES
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

  @Description("Gbif API url, by defaut is https://api.gbif.org")
  @Default.String("https://api.gbif.org")
  String getGbifApiUrl();

  void setGbifApiUrl(String path);

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
  String getEsIndexName();

  @Override
  void setEsIndexName(String esIndexName);
}
