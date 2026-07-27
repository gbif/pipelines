package org.gbif.pipelines.spark.dwcdp.builder;

import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;

/**
 * Explicit field name renames from DwC-DP to DwC-A.
 *
 * <p>DwC-DP occasionally uses field names that differ from the DwC-A term names they correspond to.
 * These differences are intentional DwC-DP design decisions, not synonyms registered in the DwC
 * standard — so {@link org.gbif.dwc.terms.TermFactory} cannot bridge them automatically. This map
 * provides the explicit mapping.
 *
 * <p>{@link TermResolver} consults this map before calling {@code TermFactory}, so any field name
 * listed here is resolved to the correct qualified URI regardless of what the library knows.
 *
 * <h2>What belongs here</h2>
 *
 * Only confirmed renames where:
 *
 * <ol>
 *   <li>The DwC-DP field name differs from the target DwC-A term's simple name, AND
 *   <li>{@code TermFactory} does not already recognise the DwC-DP name as an alternative for the
 *       target term (i.e. it is not registered as an {@code alternativeName} in the {@code DwcTerm}
 *       enum or any other loaded term vocabulary).
 * </ol>
 *
 * <h2>What does NOT belong here</h2>
 *
 * <ul>
 *   <li>Terms absent from the current {@code dwc-api} library version (e.g. {@code causeOfDeath},
 *       {@code substrate}, {@code feedbackURL}) — wait for the library to be upgraded; {@code
 *       TermFactory} will then resolve them automatically.
 *   <li>Terms that {@code TermFactory} already resolves correctly via registered alternatives.
 * </ul>
 *
 * <h2>Adding new entries</h2>
 *
 * When a new DwC-DP rename is confirmed, add it here with a comment citing the source (ingestion
 * guide section, DwC-DP schema reference, or GBIF pipeline decision). If an entry becomes redundant
 * because {@code dwc-api} adds the name as a registered alternative, remove it and note the library
 * version in which it was fixed.
 */
public final class DwcDpTermMappings {

  /**
   * DwC-DP field name → qualified DwC-A term URI.
   *
   * <p>Keyed by the exact column name as it appears in the DwC-DP Parquet file.
   */
  public static final Map<String, String> RENAMES =
      Map.of(
          // DwC-DP renamed dwc:associatedReferences to occurrenceReferences on the occurrence
          // table. Source: ingestion guide section 2, DwC-DP occurrence schema.
          "occurrenceReferences", DwcTerm.associatedReferences.qualifiedName(),

          // DwC-DP uses eventConductedBy / eventConductedByID on the event table where DwC-A
          // uses recordedBy / recordedByID. Source: ingestion guide section 1.
          "eventConductedBy", DwcTerm.recordedBy.qualifiedName(),
          "eventConductedByID", DwcTerm.recordedByID.qualifiedName());

  private DwcDpTermMappings() {}
}
