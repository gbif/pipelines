package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;

/**
 * Migration boundary for translating DwC-DP field names to qualified DwC/DwC-A terms.
 *
 * <p>Generic schema-driven copying is intentionally strict: if the legacy resolver falls back to
 * the raw source column name, that field is dropped unless it is explicitly listed in
 * {@link #RAW_OUTPUT_ALLOWLIST}. This prevents internal or otherwise unresolved DwC-DP columns from
 * silently becoming output terms.
 */
final class TargetTerms {

  /**
   * Raw extension keys deliberately retained because existing downstream extension handling consumes
   * them as part of the extension contract even though {@link TermResolver} does not qualify them.
   */
  private static final Set<String> RAW_OUTPUT_ALLOWLIST =
      Set.of(
          "identifierType",
          "identifierTypeIRI",
          "identifierTypeSource",
          "identifierLanguage",
          "isAcceptedIdentification",
          "nucleotideSequenceID",
          "nucleotideSequenceRemarks",
          "readCount",
          "processedTotalReadCount");

  static final Set<String> EVENT_CORE_RAW_OUTPUTS = Set.of("georeferencedByID");

  static final Set<String> OCCURRENCE_ENRICHMENT_RAW_OUTPUTS =
      Set.of("typeStatus", "geoName", "typeDesignationType", "geoClassificationCode");

  private TargetTerms() {}

  /** Resolves a target named explicitly by a mapping definition. */
  static String resolve(String column) {
    return TermResolver.resolve(column);
  }

  /**
   * Resolves a field for generic schema-driven copying.
   *
   * <p>An unresolved legacy fallback is recognizable because {@link TermResolver} returns the input
   * column name unchanged. Those values are pruned unless explicitly allowlisted.
   */
  static Optional<String> resolveOutput(String column) {
    return resolveOutput(column, Set.of());
  }

  static Optional<String> resolveOutput(String column, Set<String> retainedRawOutputs) {
    String resolved = TermResolver.resolve(column);
    if (!resolved.equals(column)
        || RAW_OUTPUT_ALLOWLIST.contains(column)
        || retainedRawOutputs.contains(column)) {
      return Optional.of(resolved);
    }
    return Optional.empty();
  }

  /**
   * Humboldt survey resources are themselves an extension-shaped contract: non-structural field
   * names are intentional Humboldt output keys even when the legacy resolver does not qualify them.
   */
  static Optional<String> resolveHumboldtOutput(String column) {
    return Optional.of(TermResolver.resolve(column));
  }

  static boolean isAllowedRawOutput(String column) {
    return RAW_OUTPUT_ALLOWLIST.contains(column);
  }
}
