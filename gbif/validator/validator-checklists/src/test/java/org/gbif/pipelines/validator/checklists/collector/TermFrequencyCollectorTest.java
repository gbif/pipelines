package org.gbif.pipelines.validator.checklists.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.model.NormalizedNameUsageData;
import org.gbif.pipelines.validator.checklists.model.TestData;
import org.gbif.validator.api.Metrics;
import org.junit.Test;

public class TermFrequencyCollectorTest {

  @Test
  public void termFrequencyCollectorTest() {
    // State
    NormalizedNameUsageData tapirNameUsageData = TestData.tapirNameUsageTestData();
    TermFrequencyCollector termFrequencyCollector = TermFrequencyCollector.of(tapirNameUsageData);

    // When
    List<Metrics.TermInfo> termsInfo = termFrequencyCollector.toTermsInfo();
    List<Metrics.TermInfo> distributionTermsInfo =
        termFrequencyCollector.toExtensionTermInfo(Extension.DISTRIBUTION);

    // Should

    // Extension test
    assertTrue(
        distributionTermsInfo.stream()
            .anyMatch(
                ti ->
                    ti.getRawIndexed() == 1
                        && ti.getInterpretedIndexed() == 1
                        && ti.getTerm().equals(IucnTerm.iucnRedListCategory.qualifiedName())));

    // Core terms test
    assertEquals(9, termsInfo.size());
    assertTrue(
        termsInfo.stream()
            .allMatch(ti -> ti.getRawIndexed() == 1 && ti.getInterpretedIndexed() == 1));
  }

  @Test
  public void termFrequencyTest() {
    // State
    TermFrequencyCollector.TermFrequency termFrequency = new TermFrequencyCollector.TermFrequency();

    // When
    termFrequency.inc(DwcTerm.scientificName);

    // Should
    assertEquals(1, termFrequency.getFrequency(DwcTerm.scientificName).longValue());

    // When
    termFrequency.add(DwcTerm.scientificName, 3L);
    termFrequency.add(DwcTerm.taxonomicStatus, 1L);

    // Should
    assertEquals(4, termFrequency.getFrequency(DwcTerm.scientificName).longValue());
    assertEquals(1, termFrequency.getFrequency(DwcTerm.taxonomicStatus).longValue());

    // State
    TermFrequencyCollector.TermFrequency anotherTermFrequency =
        new TermFrequencyCollector.TermFrequency();
    anotherTermFrequency.inc(DwcTerm.scientificName);

    // When
    anotherTermFrequency.add(termFrequency);

    // Should
    assertEquals(5L, termFrequency.getFrequency(DwcTerm.scientificName) + 1);

    // State
    Map<Term, Long> frequencies = Collections.singletonMap(DwcTerm.scientificName, 2L);

    // When
    anotherTermFrequency.add(frequencies);

    // Should
    assertEquals(7L, anotherTermFrequency.getFrequency(DwcTerm.scientificName).longValue());

    // State
    List<Map<Term, Long>> frequenciesList = new ArrayList<>();
    frequenciesList.add(frequencies);
    frequenciesList.add(Collections.singletonMap(DwcTerm.lifeStage, 1L));

    // When
    anotherTermFrequency.add(frequenciesList);

    // Should
    assertEquals(9L, anotherTermFrequency.getFrequency(DwcTerm.scientificName).longValue());
    assertEquals(1L, anotherTermFrequency.getFrequency(DwcTerm.lifeStage).longValue());
  }
}
