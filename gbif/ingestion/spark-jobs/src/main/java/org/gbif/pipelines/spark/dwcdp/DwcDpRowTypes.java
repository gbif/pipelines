package org.gbif.pipelines.spark.dwcdp;

import org.gbif.dwc.terms.DwcTerm;

/**
 * Qualified row type URIs used when building and reading {@link
 * org.gbif.pipelines.io.avro.ExtendedRecord} core and extension maps for DwC-DP datasets.
 *
 * <p>Centralised here so that builders, the orchestrator, and tests all reference the same
 * constants rather than each declaring their own.
 */
public final class DwcDpRowTypes {

  public static final String CORE_ROW_TYPE_EVENT = DwcTerm.Event.qualifiedName();
  public static final String CORE_ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  /** Extension row type for occurrences attached to an event core. */
  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  /** Extension row type for multimedia records. */
  public static final String ROW_TYPE_MULTIMEDIA = "http://rs.tdwg.org/ac/terms/Multimedia";

  private DwcDpRowTypes() {}
}
