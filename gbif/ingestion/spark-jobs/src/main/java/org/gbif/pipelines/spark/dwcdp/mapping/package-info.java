/**
 * DwC-DP to DwC-A mapping framework.
 *
 * <p>The framework is organized by lifecycle:
 *
 * <ul>
 *   <li>{@code definition}: declarative mapping plans and their DSL.
 *   <li>{@code schema}: schema graph, resources, relations and paths.
 *   <li>{@code compilation}: schema resolution, compiled mapping IR, pruning and diagnostics.
 *   <li>{@code execution}: Spark materialization, projection, caching and execution metrics.
 *   <li>{@code config}: GBIF's concrete DwC-DP to DwC-A mapping declarations.
 *   <li>{@code engine}: orchestration facade used by conversion code.
 * </ul>
 */
package org.gbif.pipelines.spark.dwcdp.mapping;
