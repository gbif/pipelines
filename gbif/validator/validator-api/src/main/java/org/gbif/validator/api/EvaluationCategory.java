package org.gbif.validator.api;

/** Used to group {@link EvaluationType} into categories. */
public enum EvaluationCategory {
  /** RESOURCE_INTEGRITY is a category of EvaluationType that stops the evaluation process */
  RESOURCE_INTEGRITY,
  RESOURCE_STRUCTURE,
  METADATA_CONTENT,
  RECORD_STRUCTURE,
  OCC_INTERPRETATION_BASED,
  CLB_INTERPRETATION_BASED
}
