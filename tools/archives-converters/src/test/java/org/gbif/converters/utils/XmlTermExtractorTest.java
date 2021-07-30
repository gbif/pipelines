package org.gbif.converters.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class XmlTermExtractorTest {

  private String getTestInputPath() {
    return getClass()
        .getResource("/responses/pages/7ef15372-1387-11e2-bb2e-00145eb45e9a/")
        .getFile();
  }

  @Test
  public void extractXmlTermsTest() throws IOException {

    // State
    String inputPath = getTestInputPath() + "61";
    List<File> files = XmlFilesReader.getInputFiles(new File(inputPath));

    // When
    XmlTermExtractor extractor = XmlTermExtractor.extract(files);

    // Should
    Set<Term> core = extractor.getCore();
    assertEquals(12, core.size());
    assertTrue(core.contains(DwcTerm.collectionCode));

    Map<Extension, Set<Term>> extenstionsTerms = extractor.getExtenstionsTerms();
    assertTrue(extenstionsTerms.isEmpty());
  }
}
