package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.junit.jupiter.api.Test;

class TargetTermsTest {

  @Test
  void qualifiedTermsRemainEligibleForGenericOutput() {
    assertEquals(
        DwcTerm.occurrenceID.qualifiedName(),
        TargetTerms.resolveOutput("occurrenceID").orElseThrow());
  }

  @Test
  void unresolvedRawNamesArePrunedFromGenericOutput() {
    assertTrue(TargetTerms.resolveOutput("definitelyNotADwcTerm").isEmpty());
  }

  @Test
  void domainScopedRawOutputsDoNotBecomeGlobalAllowlistEntries() {
    assertTrue(TargetTerms.resolveOutput("georeferencedByID").isEmpty());
    assertEquals(
        "georeferencedByID",
        TargetTerms.resolveOutput("georeferencedByID", TargetTerms.EVENT_CORE_RAW_OUTPUTS)
            .orElseThrow());
  }

  @Test
  void humboldtResourceContractMayRetainUnqualifiedNonStructuralTerms() {
    assertEquals(
        "surveyTargetDescription",
        TargetTerms.resolveHumboldtOutput("surveyTargetDescription").orElseThrow());
  }

  @Test
  void humboldtOverlappingAgentTermsUseEcoNamespace() {
    assertEquals(
        EcoTerm.identifiedBy.qualifiedName(),
        TargetTerms.resolveHumboldtOutput("identifiedBy").orElseThrow());
    assertEquals(
        EcoTerm.identificationReferences.qualifiedName(),
        TargetTerms.resolveHumboldtOutput("identificationReferences").orElseThrow());
  }

  @Test
  void intentionalRawExtensionKeysAreExplicitlyAllowlisted() {
    assertEquals("readCount", TargetTerms.resolveOutput("readCount").orElseThrow());
    assertEquals(
        "isAcceptedIdentification",
        TargetTerms.resolveOutput("isAcceptedIdentification").orElseThrow());
    assertTrue(TargetTerms.isAllowedRawOutput("readCount"));
  }
}
