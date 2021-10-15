package org.gbif.pipelines.validator.checklists.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.gbif.api.model.checklistbank.Distribution;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.api.vocabulary.ThreatStatus;
import org.gbif.checklistbank.model.UsageExtensions;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;

@UtilityClass
public class TestData {

  public static NormalizedNameUsageData tapirNameUsageTestData() {
    // NameUsage
    NameUsage nameUsage = new NameUsage();
    nameUsage.setScientificName("Tapirus bairdii (Gill, 1865)");
    nameUsage.setTaxonomicStatus(TaxonomicStatus.ACCEPTED);
    nameUsage.setKingdom("Metazoa");
    nameUsage.setPhylum("Chordata");
    nameUsage.setClazz("Mammalia");
    nameUsage.setOrder("Perissodactyla");
    nameUsage.setFamily("Tapiridae");
    nameUsage.setGenus("Tapirus");
    nameUsage.setRank(Rank.SPECIES);

    // Verbatim
    VerbatimNameUsage verbatimNameUsage = new VerbatimNameUsage();
    verbatimNameUsage.setCoreField(DwcTerm.scientificName, "Tapirus bairdii (Gill, 1865)");
    verbatimNameUsage.setCoreField(DwcTerm.kingdom, "Metazoa");
    verbatimNameUsage.setCoreField(DwcTerm.phylum, "Chordata");
    verbatimNameUsage.setCoreField(DwcTerm.class_, "Mammalia");
    verbatimNameUsage.setCoreField(DwcTerm.order, "Perissodactyla");
    verbatimNameUsage.setCoreField(DwcTerm.family, "Tapiridae");
    verbatimNameUsage.setCoreField(DwcTerm.genus, "Tapirus");
    verbatimNameUsage.setCoreField(DwcTerm.taxonRank, "species");
    verbatimNameUsage.setCoreField(DwcTerm.taxonomicStatus, "accepted");

    // Distribution extension
    Distribution distribution = new Distribution();
    distribution.setThreatStatus(ThreatStatus.ENDANGERED);
    UsageExtensions usageExtensions = new UsageExtensions();
    usageExtensions.distributions.add(distribution);

    // Verbatim extensions
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
}
