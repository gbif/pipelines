package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Options for running UUID based pipelines. */
public interface UUIDPipelineOptions extends InterpretationPipelineOptions {

  @Description("The number of backups to keep")
  @Default.Integer(10)
  Integer getNumberOfBackupsToKeep();

  void setNumberOfBackupsToKeep(Integer numberOfBackupsToKeep);

  @Description("The permitted percentage change in UUIDs")
  @Default.Integer(50)
  Integer getPercentageChangeAllowed();

  void setPercentageChangeAllowed(Integer percentageChangeAllowed);

  @Description("Override permitted percentage change in UUIDs check")
  @Default.Boolean(false)
  Boolean getOverridePercentageCheck();

  void setOverridePercentageCheck(Boolean overridePercentageCheck);

  @Description("Throw error on validation fail")
  @Default.Boolean(false)
  Boolean getThrowErrorOnValidationFail();

  void setThrowErrorOnValidationFail(Boolean throwErrorOnValidationFail);
}
