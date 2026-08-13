package org.gbif.pipelines.spark.dwcdp.mapping.config;

import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;

/**
 * Temporary migration boundary for translating DwC-DP field names to the same qualified DwC/DwC-A
 * terms used by the legacy builders.
 *
 * <p>Keeping this adapter in config means mapping definitions do not depend directly on the legacy
 * builder package. Once all mappings have parity and the old implementation is removed, the
 * underlying term-resolution implementation can move behind this boundary without changing the
 * mapping configs.
 */
final class TargetTerms {
  private TargetTerms() {}

  static String resolve(String column) {
    return TermResolver.resolve(column);
  }
}
