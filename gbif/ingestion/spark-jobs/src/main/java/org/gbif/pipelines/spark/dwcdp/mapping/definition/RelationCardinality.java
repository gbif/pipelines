package org.gbif.pipelines.spark.dwcdp.mapping.definition;

/** Cardinality of a schema relation when traversed from source to target. */
public enum RelationCardinality {
  ONE_TO_ONE,
  ONE_TO_MANY,
  MANY_TO_ONE,
  MANY_TO_MANY,
  UNKNOWN;

  public boolean canMultiplyRows() {
    return this == ONE_TO_MANY || this == MANY_TO_MANY || this == UNKNOWN;
  }
}
