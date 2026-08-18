package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;

/** Migration boundary for translating DwC-DP field names to qualified DwC/DwC-A terms. */
final class TargetTerms {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  /** Confirmed DwC-DP source-name to DwC-A target-term renames. */
  private static final Map<String, String> RENAMES =
      Map.of(
          "occurrenceReferences", DwcTerm.associatedReferences.qualifiedName(),
          "eventConductedBy", DwcTerm.recordedBy.qualifiedName(),
          "eventConductedByID", DwcTerm.recordedByID.qualifiedName(),
          "accessURI", DcTerm.identifier.qualifiedName(),
          "mediaType", DcTerm.type.qualifiedName());


  /** Humboldt fields whose simple names overlap terms in other namespaces. */
  private static final Map<String, String> HUMBOLDT_RENAMES =
      Map.of(
          "identifiedBy", EcoTerm.identifiedBy.qualifiedName(),
          "identificationReferences", EcoTerm.identificationReferences.qualifiedName());

  /** Raw extension keys deliberately retained as part of their current extension contracts. */
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
      Set.of("geoName", "typeDesignationType", "geoClassificationCode");

  private TargetTerms() {}

  /** Resolves a target named explicitly by a mapping definition. */
  static String resolve(String column) {
    return resolveKnown(column).orElse(column);
  }

  static Optional<String> resolveOutput(String column) {
    return resolveOutput(column, Set.of());
  }

  static Optional<String> resolveOutput(String column, Set<String> retainedRawOutputs) {
    Optional<String> resolved = resolveKnown(column);
    if (resolved.isPresent()) {
      return resolved;
    }
    if (RAW_OUTPUT_ALLOWLIST.contains(column) || retainedRawOutputs.contains(column)) {
      return Optional.of(column);
    }
    return Optional.empty();
  }

  /** Humboldt resource fields form an extension contract; known terms are qualified, others retained. */
  static Optional<String> resolveHumboldtOutput(String column) {
    String renamed = HUMBOLDT_RENAMES.get(column);
    return Optional.of(renamed != null ? renamed : resolveKnown(column).orElse(column));
  }

  static boolean isAllowedRawOutput(String column) {
    return RAW_OUTPUT_ALLOWLIST.contains(column);
  }

  private static Optional<String> resolveKnown(String column) {
    String renamed = RENAMES.get(column);
    if (renamed != null) {
      return Optional.of(renamed);
    }
    try {
      Term term = TERM_FACTORY.findTerm(column);
      if (term != null && !(term instanceof UnknownTerm)) {
        return Optional.of(term.qualifiedName());
      }
    } catch (RuntimeException ignored) {
      // Unknown names are handled by the caller's explicit raw-output policy.
    }
    return Optional.empty();
  }
}
