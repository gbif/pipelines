package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

public interface SamplingPipelineOptions extends InterpretationPipelineOptions {

  @Description("Sampling Service Url")
  String getSamplingServiceUrl();

  void setSamplingServiceUrl(String var1);

  @Description("Sampling Service batch size")
  @Default.Integer(25000)
  Integer getBatchSize();

  void setBatchSize(Integer batchSize);

  @Description("Sampling Service batch size")
  @Default.Integer(1000)
  Integer getBatchSleepTimeInMillis();

  void setBatchSleepTimeInMillis(Integer sleepTime);

  @Description("Sampling Service batch size")
  @Default.Integer(5)
  Integer getDownloadRetries();

  void setDownloadRetries(Integer downloadRetries);
}
