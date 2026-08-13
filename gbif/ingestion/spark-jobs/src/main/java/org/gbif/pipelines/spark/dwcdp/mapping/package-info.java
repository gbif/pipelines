/**
 * Declarative DwC-DP mapping model.
 *
 * <p>This package intentionally models mapping semantics rather than mirroring Spark's DataFrame API:
 * schema-driven relations, predicates, explicit cardinality handling, competing/additive target
 * sources, validation and eventually instrumentation. Spark compilation belongs in a separate
 * execution layer built from these model objects.
 */
package org.gbif.pipelines.spark.dwcdp.mapping;
