package org.gbif.pipelines.backbone.impact;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface NameServiceComparisonOptions extends BackbonePreReleaseOptions {

  @Description("Base URL for the API, e.g. https://api.gbif-uat.org/v1/")
  String getNewAPIBaseURI();

  void setNewAPIBaseURI(String newBaseUri);

  @Description(
      "Match against the checklistbank API and use the specified dataset key for name usage mapping")
  @Default.String("")
  String getNewClbDatasetKey();

  void setNewClbDatasetKey(String newClbDatasetKey);
}
