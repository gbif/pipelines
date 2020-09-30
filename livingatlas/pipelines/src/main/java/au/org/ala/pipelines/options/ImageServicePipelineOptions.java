package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

public interface ImageServicePipelineOptions extends InterpretationPipelineOptions {

  @Description("Image Service Url")
  String getImageServiceUrl();

  void setImageServiceUrl(String imageServiceUrl);

  @Description("Image Service sleep time between polling")
  @Default.Integer(5000)
  Integer getSleepTimeInMillis();

  void setSleepTimeInMillis(Integer sleepTime);

  @Description("Use async uploads")
  @Default.Boolean(false)
  boolean isAsyncUpload();

  void setAsyncUpload(boolean asyncUpload);

  @Description("The number of days to synchronise from")
  @Default.Long(-1)
  Long getModifiedWindowTimeInDays();

  void setModifiedWindowTimeInDays(Long modifiedWindowTimeInDays);
}
