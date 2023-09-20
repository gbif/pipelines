package au.org.ala.pipelines.common;

import org.gbif.api.model.pipelines.InterpretationType;

/** ALA extensions to PipelinesVariables.Pipeline.Interpretation.InterpretationType. */
public enum ALARecordTypes implements InterpretationType {
  ALL,
  ALA_UUID,
  ALA_TAXONOMY,
  ALA_ATTRIBUTION,
  ALA_SENSITIVE_DATA,
  ALA_DISTRIBUTION,
  JACKKNIFE_OUTLIER,
  SEEDBANK,
  RECORD_ANNOTATION;

  ALARecordTypes() {}

  @Override
  public String all() {
    return ALL.name();
  }
}
