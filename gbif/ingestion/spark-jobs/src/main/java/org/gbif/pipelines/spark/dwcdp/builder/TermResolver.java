package org.gbif.pipelines.spark.dwcdp.builder;

import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;

/**
 * Resolves DwC-DP column names to qualified DwC term URIs.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>{@link DwcDpTermMappings#RENAMES} — explicit DwC-DP field name renames where the column
 *       name differs from the DwC-A term name and {@code TermFactory} cannot bridge the gap.
 *   <li>{@link TermFactory} — the standard DwC term registry, which covers all terms in the current
 *       {@code dwc-api} library version including registered alternative names.
 *   <li>Fall-through — if neither step resolves the name, the raw column name is returned as-is.
 *       This covers terms present in the DwC standard but not yet in the library version (e.g.
 *       {@code causeOfDeath}) and genuinely non-standard publisher-specific columns. Both are
 *       passed through rather than dropped: downstream interpretation will simply ignore keys it
 *       does not recognise, and the data is preserved for future re-ingestion once the library is
 *       upgraded.
 * </ol>
 */
@Slf4j
public final class TermResolver {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private TermResolver() {}

  /**
   * Resolves {@code columnName} to a qualified term URI.
   *
   * @param columnName the DwC-DP column name as it appears in the Parquet file
   * @return the qualified term URI if resolvable, otherwise the raw column name
   */
  public static String resolve(String columnName) {
    // Step 1: explicit DwC-DP renames
    String renamed = DwcDpTermMappings.RENAMES.get(columnName);
    if (renamed != null) {
      log.debug("Resolved '{}' via rename map → {}", columnName, renamed);
      return renamed;
    }

    // Step 2: standard TermFactory lookup
    try {
      Term term = TERM_FACTORY.findTerm(columnName);
      if (term != null && !(term instanceof UnknownTerm)) {
        return term.qualifiedName();
      }
    } catch (Exception e) {
      log.debug("TermFactory could not resolve '{}', falling through to raw name", columnName);
    }

    // Step 3: fall-through — return raw column name
    log.debug("No resolution found for '{}', keeping raw column name", columnName);
    return columnName;
  }
}
