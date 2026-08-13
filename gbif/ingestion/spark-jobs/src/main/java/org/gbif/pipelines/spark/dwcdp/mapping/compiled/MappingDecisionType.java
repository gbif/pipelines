package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

/** Why the compiler selected or rejected target producers. */
public enum MappingDecisionType {
  SINGLE_PRODUCER,
  EXPLICIT_OVERRIDE,
  INFERRED_CLOSEST,
  AMBIGUOUS_MULTIPLE_EXPLICIT,
  AMBIGUOUS_EQUAL_DEPTH
}
