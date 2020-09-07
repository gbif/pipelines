package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.*;

public interface AllDatasetsPipelinesOptions extends PipelineOptions {

  @Description("Path of the input file.")
  String getInputPath();

  void setInputPath(String var1);

  @Description("Target path where the outputs of the pipeline will be written to. Required.")
  @Validation.Required
  @Default.InstanceFactory(
      org.gbif.pipelines.ingest.options.BasePipelineOptions.DefaultDirectoryFactory.class)
  String getTargetPath();

  void setTargetPath(String var1);

  @Description(
      "Target metadata file name where the outputs of the pipeline metrics results will be written to.")
  String getMetaFileName();

  void setMetaFileName(String var1);

  @Description("If set to true it writes the outputs of every step of the pipeline")
  @Default.Boolean(false)
  boolean getWriteOutput();

  void setWriteOutput(boolean var1);

  @Description("Avro compression type")
  @org.apache.beam.sdk.options.Default.String("snappy")
  String getAvroCompressionType();

  void setAvroCompressionType(String var1);

  @Description("Avro sync interval time")
  @org.apache.beam.sdk.options.Default.Integer(2097152)
  int getAvroSyncInterval();

  void setAvroSyncInterval(int var1);

  @Description("The threshold for java based pipelines, switches between sync and async execution")
  @org.apache.beam.sdk.options.Default.Integer(1000)
  int getSyncThreshold();

  void setSyncThreshold(int var1);
}
