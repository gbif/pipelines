package uk.org.nbn.pipelines.common;

import org.gbif.api.model.pipelines.InterpretationType;

/** NBN extensions to PipelinesVariables.Pipeline.Interpretation.InterpretationType. */
public enum NBNRecordTypes implements InterpretationType {
  ALL,
  NBN_ACCESS_CONTROLLED_DATA;

  NBNRecordTypes() {}

  @Override
  public String all() {
    return ALL.name();
  }
}
