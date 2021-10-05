package org.gbif.pipelines.validator.checklists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.model.checklistbank.Distribution;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.ThreatStatus;
import org.gbif.checklistbank.model.UsageExtensions;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.validator.api.Metrics;
import org.junit.Assert;
import org.junit.Test;

public class TermFrequencyCollectorTest {

  public NormalizedNameUsageData normalizedNameUsageData() {
    NameUsage nameUsage = new NameUsage();
    nameUsage.setScientificName("Tapirus bairdii (Gill, 1865)");
    VerbatimNameUsage verbatimNameUsage = new VerbatimNameUsage();
    verbatimNameUsage.setCoreField(DwcTerm.scientificName, "Tapirus bairdii (Gill, 1865)");
    Distribution distribution = new Distribution();
    distribution.setThreatStatus(ThreatStatus.ENDANGERED);
    UsageExtensions usageExtensions = new UsageExtensions();
    usageExtensions.distributions.add(distribution);
    Map<Extension, List<Map<Term, String>>> extensions = new HashMap<>();
    extensions.put(
        Extension.DISTRIBUTION,
        Collections.singletonList(
            Collections.singletonMap(IucnTerm.iucnRedListCategory, "endangered")));
    verbatimNameUsage.setExtensions(extensions);
    return NormalizedNameUsageData.builder()
        .nameUsage(nameUsage)
        .verbatimNameUsage(verbatimNameUsage)
        .usageExtensions(usageExtensions)
        .build();
  }

  @Test
  public void frequencyTest() {
    NormalizedNameUsageData normalizedNameUsageData = normalizedNameUsageData();
    TermFrequencyCollector termFrequencyCollector =
        TermFrequencyCollector.of(normalizedNameUsageData);

    List<Metrics.TermInfo> termsInfo = termFrequencyCollector.toTermsInfo();
    List<Metrics.TermInfo> distributionTermsInfo =
        termFrequencyCollector.toExtensionTermInfo(Extension.DISTRIBUTION);

    // Distribution extensions
    Assert.assertTrue(
        distributionTermsInfo.stream()
            .anyMatch(
                ti ->
                    ti.getRawIndexed() == 1
                        && ti.getInterpretedIndexed() == 1
                        && ti.getTerm().equals(IucnTerm.iucnRedListCategory.qualifiedName())));

    // Core
    Assert.assertTrue(
        termsInfo.stream()
            .anyMatch(
                ti ->
                    ti.getTerm().equals(DwcTerm.scientificName.qualifiedName())
                        && ti.getRawIndexed() == 1
                        && ti.getInterpretedIndexed() == 1));
  }
}
