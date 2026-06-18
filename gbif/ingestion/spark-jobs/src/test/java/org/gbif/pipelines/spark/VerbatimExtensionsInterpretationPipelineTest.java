package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Simple regression test for {@link
 * VerbatimExtensionsInterpretationPipeline#normalizeFieldName(String)}.
 */
public class VerbatimExtensionsInterpretationPipelineTest {

  @Test
  public void testNormalizeFieldNameRemovesUnderscores() {
    assertEquals(
        "dnasequence",
        VerbatimExtensionsInterpretationPipeline.normalizeFieldName(
            "http://rs.gbif.org/terms/dna_sequence"));
  }
}
