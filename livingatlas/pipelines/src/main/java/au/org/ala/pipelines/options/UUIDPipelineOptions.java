package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

public interface UUIDPipelineOptions extends InterpretationPipelineOptions {

  @Description("Allow UUIDs to be created for datasets with empty unique terms specified")
  @Default.Boolean(false)
  boolean isAllowEmptyUniqueTerms();

  void setAllowEmptyUniqueTerms(boolean allowEmptyUniqueTerms);
}
