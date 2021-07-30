package org.gbif.pipelines.core.utils;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.junit.Test;

public class DwcaTermUtilsTest {

  @Test
  public void readNoExtensionsAsTermsTest() {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    Archive archive = DwcaUtils.fromLocation(Paths.get(fileName));
    Set<String> result = DwcaUtils.getExtensionAsTerms(archive);

    // Should
    assertTrue(result.isEmpty());
  }

  @Test
  public void readExtensionsAsTermsTest() {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    Archive archive = DwcaUtils.fromLocation(Paths.get(fileName));
    Set<String> result = DwcaUtils.getExtensionAsTerms(archive);

    // Should
    assertTrue(result.contains(IDENTIFIER_TABLE.name()));
  }

  @Test
  public void readCoreTermsTest() {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    Archive archive = DwcaUtils.fromLocation(Paths.get(fileName));
    Set<Term> result = DwcaUtils.getCoreTerms(archive);

    // Should
    assertEquals(6, result.size());
    assertTrue("dwc:family", result.contains(DwcTerm.family));
    assertTrue("dwc:institutionCode", result.contains(DwcTerm.institutionCode));
    assertTrue("dwc:collectionCode", result.contains(DwcTerm.collectionCode));
    assertTrue("dwc:scientificName", result.contains(DwcTerm.scientificName));
    assertTrue("dwc:basisOfRecord", result.contains(DwcTerm.basisOfRecord));
    assertTrue("dwc:kingdom", result.contains(DwcTerm.kingdom));
  }

  @Test
  public void readExtensionsTermsTest() {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    Archive archive = DwcaUtils.fromLocation(Paths.get(fileName));
    Map<Extension, Set<Term>> result = DwcaUtils.getExtensionsTerms(archive);

    // Should
    assertEquals(1, result.size());
    assertTrue("IDENTIFIER", result.containsKey(Extension.IDENTIFIER));
    assertTrue("dc:identifier", result.get(Extension.IDENTIFIER).contains(DcTerm.identifier));
  }
}
