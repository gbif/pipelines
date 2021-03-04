package org.gbif.pipelines.core.utils;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;
import org.gbif.api.vocabulary.Extension;
import org.junit.Test;

public class DwcaExtensionTermUtilsTest {

  @Test
  public void readNoExtensionsTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();

    // When
    Set<String> result = DwcaExtensionTermUtils.fromLocation(Paths.get(fileName));

    // Should
    assertTrue(result.isEmpty());
  }

  @Test
  public void readExtensionsTest() throws IOException {
    // State
    String fileName = getClass().getResource("/dwca/plants_dwca_ext").getFile();

    // When
    Set<String> result = DwcaExtensionTermUtils.fromLocation(Paths.get(fileName));

    // Should
    assertTrue(result.contains(Extension.IDENTIFIER.getRowType()));
  }
}
