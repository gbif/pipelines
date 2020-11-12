package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface DwcaToVerbatimPipelineOptions extends InterpretationPipelineOptions {

  @Description("Delete lock file on exit")
  @Default.Boolean(true)
  boolean isDeleteLockFileOnExit();

  void setDeleteLockFileOnExit(boolean deleteLockFileOnExit);
}
