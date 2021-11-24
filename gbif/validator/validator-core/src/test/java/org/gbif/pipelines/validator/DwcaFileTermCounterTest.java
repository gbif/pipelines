package org.gbif.pipelines.validator;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.junit.Test;

public class DwcaFileTermCounterTest {

  @Test
  public void eventCounterTest() throws Exception {

    // State
    String path =
        getClass().getResource("/dwca_counter/9997fa4e-54c1-43ea-9856-afa90204c162").getFile();
    Archive archive = DwcFiles.fromLocation(Paths.get(path));

    // When
    List<FileInfo> result = DwcaFileTermCounter.process(archive);

    // Shoud
    assertEquals(3, result.size());
    assertFileSize(result, "event.txt", 30L);
    assertTermSize(result, "event.txt", "http://rs.tdwg.org/dwc/terms/month", 30L);

    assertFileSize(result, "measurementorfact.txt", 1L);
    assertTermSize(
        result, "measurementorfact.txt", "http://rs.tdwg.org/dwc/terms/measurementValue", 1L);

    assertFileSize(result, "occurrence.txt", 2L);
    assertTermSize(result, "occurrence.txt", "http://rs.tdwg.org/dwc/terms/family", 2L);
    assertTermSize(result, "occurrence.txt", "http://rs.tdwg.org/dwc/terms/genus", 1L);
  }

  private void assertFileSize(List<FileInfo> result, String fileName, Long expectedSize) {
    Optional<FileInfo> first =
        result.stream().filter(x -> x.getFileName().equals(fileName)).findFirst();

    assertTrue(first.isPresent());
    assertEquals(expectedSize, first.get().getCount());
  }

  private void assertTermSize(
      List<FileInfo> result, String fileName, String term, Long expectedSize) {
    Optional<FileInfo> first =
        result.stream().filter(x -> x.getFileName().equals(fileName)).findFirst();

    assertTrue(first.isPresent());

    Optional<TermInfo> termInfo =
        first.get().getTerms().stream().filter(x -> x.getTerm().equals(term)).findFirst();
    assertTrue(termInfo.isPresent());
    assertEquals(expectedSize, termInfo.get().getRawIndexed());
  }
}
