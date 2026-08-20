package org.gbif.pipelines.spark.dwcdp.mapping.definition;

/** Whether absence of a resource on a mapping path is tolerated at execution time. */
public enum RelationRequirement {
  OPTIONAL,
  REQUIRED
}
