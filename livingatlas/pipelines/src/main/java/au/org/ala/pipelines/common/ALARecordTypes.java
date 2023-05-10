package au.org.ala.pipelines.common;

import org.gbif.pipelines.common.PipelinesVariables;

/** ALA extensions to PipelinesVariables.Pipeline.Interpretation.InterpretationType. */
public enum ALARecordTypes
    implements PipelinesVariables.Pipeline.Interpretation.InterpretationType {
  ALL,
  ALA_UUID,
  ALA_TAXONOMY,
  ALA_ATTRIBUTION,
  ALA_SENSITIVE_DATA,
  ALA_DISTRIBUTION,
  JACKKNIFE_OUTLIER,
  SEEDBANK;

  ALARecordTypes() {}

  @Override
  public String all() {
    return ALL.name();
  }
}
