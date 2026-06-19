package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;

import org.gbif.dwc.terms.DcElement;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.junit.Test;

public class VerbatimExtensionsInterpretationPipelineTest {

  @Test
  public void dnaExtensionTermsTest() {
    assertEquals(
        "dnasequence",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(
            GbifDnaTerm.dna_sequence.qualifiedName()));
    assertEquals(
        "targetgene",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(
            MixsTerm.target_gene.qualifiedName()));
    assertEquals(
        "_16srecover",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(
            MixsTerm._16s_recover.qualifiedName()));
  }

  @Test
  public void audubonExtensionTermsTest() {
    assertEquals(
        "dc_type",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(
            DcElement.type.qualifiedName()));
    assertEquals(
        "dcterms_type",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(DcTerm.type.qualifiedName()));
  }
}
