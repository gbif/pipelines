package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

/** Why the compiler selected or rejected target producers. */
public enum MappingDecisionType {
  SINGLE_PRODUCER,
  EXPLICIT_OVERRIDE,
  INFERRED_CLOSEST,
  EXPLICIT_MERGE,
  MISSING_MERGE_PRODUCERS,
  MISSING_FRAGMENT_SCOPE,
  INVALID_RELATION,
  AMBIGUOUS_MULTIPLE_EXPLICIT,
  AMBIGUOUS_EQUAL_DEPTH
}
