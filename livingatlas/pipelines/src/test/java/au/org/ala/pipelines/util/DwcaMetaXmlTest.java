package au.org.ala.pipelines.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.junit.Assert;
import org.junit.Test;

public class DwcaMetaXmlTest {

  private final String path = getClass().getResource("/metaxml").getPath();

  @Test
  public void writerTest() throws IOException {
    // State
    String file = "core-multimedia.xml";

    List<String> core =
        Arrays.asList(
            DwcTerm.occurrenceID.qualifiedName(), DwcTerm.occurrenceStatus.qualifiedName());

    List<String> multimedia =
        Arrays.asList(DcTerm.identifier.qualifiedName(), DcTerm.contributor.qualifiedName());

    // When
    DwcaMetaXml.builder()
        .coreTerms(core)
        .multimediaTerms(multimedia)
        .pathToWrite(path + "/test-" + file)
        .create()
        .write();

    // Should
    List<String> expected = Files.readAllLines(Paths.get(path, file));
    List<String> result = Files.readAllLines(Paths.get(path, "test-" + file));

    Assert.assertNotNull(result);
    Assert.assertEquals(expected.size(), result.size());
    for (int x = 0; x < expected.size(); x++) {
      Assert.assertEquals(expected.get(0), result.get(0));
    }
  }

  @Test
  public void writerEmptyMultimediaTest() throws IOException {
    // State
    String file = "empty-multimedia.xml";

    List<String> core =
        Arrays.asList(
            DwcTerm.occurrenceID.qualifiedName(), DwcTerm.occurrenceStatus.qualifiedName());

    List<String> multimedia = Collections.emptyList();

    // When
    DwcaMetaXml.builder()
        .coreTerms(core)
        .multimediaTerms(multimedia)
        .pathToWrite(path + "/test-" + file)
        .create()
        .write();

    // Should
    List<String> expected = Files.readAllLines(Paths.get(path, file));
    List<String> result = Files.readAllLines(Paths.get(path, "test-" + file));

    Assert.assertNotNull(result);
    Assert.assertEquals(expected.size(), result.size());
    for (int x = 0; x < expected.size(); x++) {
      Assert.assertEquals(expected.get(0), result.get(0));
    }
  }
}
