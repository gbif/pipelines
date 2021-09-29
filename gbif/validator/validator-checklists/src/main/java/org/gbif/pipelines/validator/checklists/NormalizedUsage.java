package org.gbif.pipelines.validator.checklists;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.checklistbank.model.UsageExtensions;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

@Data
@Builder
/** Object holder for the results of Checklists normalization. */
public class NormalizedUsage {

  private final VerbatimNameUsage verbatimNameUsage;

  private final NameUsage nameUsage;

  private final ParsedName parsedName;

  private final UsageExtensions usageExtensions;

  public Map<Term, String> toTermMap() {
    Map<Term, String> termMap = toTermMap(nameUsage);
    termMap.putAll(toTermMap(parsedName));
    return termMap;
  }

  public Map<Term, String> verbatimTermsMap() {
    return verbatimNameUsage.getFields();
  }

  /** Converts a NameUsage to a Map<Term,String>. */
  private Map<Term, String> toTermMap(NameUsage nameUsage) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(nameUsage.getAccepted())
        .ifPresent(v -> termsMap.put(DwcTerm.acceptedNameUsage, v));

    Optional.ofNullable(nameUsage.getAuthorship())
        .ifPresent(v -> termsMap.put(DwcTerm.scientificNameAuthorship, v));

    Optional.ofNullable(nameUsage.getAccordingTo())
        .ifPresent(v -> termsMap.put(DwcTerm.nameAccordingTo, v));

    Optional.ofNullable(nameUsage.getCanonicalName())
        .ifPresent(v -> termsMap.put(GbifTerm.canonicalName, v));

    Optional.ofNullable(nameUsage.getNameType())
        .ifPresent(v -> termsMap.put(GbifTerm.nameType, v.name()));

    Optional.ofNullable(nameUsage.getPublishedIn())
        .ifPresent(v -> termsMap.put(DwcTerm.namePublishedIn, v));

    Optional.ofNullable(nameUsage.getTaxonID()).ifPresent(v -> termsMap.put(DwcTerm.taxonID, v));

    Optional.ofNullable(nameUsage.getVernacularName())
        .ifPresent(v -> termsMap.put(DwcTerm.vernacularName, v));

    Optional.ofNullable(nameUsage.getTaxonomicStatus())
        .ifPresent(v -> termsMap.put(DwcTerm.taxonomicStatus, v.name()));

    Optional.ofNullable(nameUsage.getNomenclaturalStatus())
        .ifPresent(v -> termsMap.put(DwcTerm.nomenclaturalStatus, toString(v)));

    Optional.ofNullable(nameUsage.getKingdom()).ifPresent(v -> termsMap.put(DwcTerm.kingdom, v));

    Optional.ofNullable(nameUsage.getPhylum()).ifPresent(v -> termsMap.put(DwcTerm.phylum, v));

    Optional.ofNullable(nameUsage.getClazz()).ifPresent(v -> termsMap.put(DwcTerm.class_, v));

    Optional.ofNullable(nameUsage.getOrder()).ifPresent(v -> termsMap.put(DwcTerm.order, v));

    Optional.ofNullable(nameUsage.getFamily()).ifPresent(v -> termsMap.put(DwcTerm.family, v));

    Optional.ofNullable(nameUsage.getGenus()).ifPresent(v -> termsMap.put(DwcTerm.genus, v));

    Optional.ofNullable(nameUsage.getSubgenus()).ifPresent(v -> termsMap.put(DwcTerm.subgenus, v));

    Optional.ofNullable(nameUsage.getSpecies()).ifPresent(v -> termsMap.put(GbifTerm.species, v));

    Optional.ofNullable(nameUsage.getRank())
        .ifPresent(v -> termsMap.put(DwcTerm.taxonRank, v.name()));

    Optional.ofNullable(nameUsage.getRemarks())
        .ifPresent(v -> termsMap.put(DwcTerm.taxonRemarks, v));

    Optional.ofNullable(nameUsage.getParent())
        .ifPresent(v -> termsMap.put(DwcTerm.parentNameUsage, v));

    Optional.ofNullable(nameUsage.getScientificName())
        .ifPresent(v -> termsMap.put(DwcTerm.scientificName, v));

    Optional.ofNullable(nameUsage.getBasionym())
        .ifPresent(v -> termsMap.put(DwcTerm.originalNameUsage, v));

    Optional.ofNullable(nameUsage.getReferences())
        .ifPresent(v -> termsMap.put(DcTerm.references, v.toString()));

    Optional.ofNullable(nameUsage.getVernacularName())
        .ifPresent(v -> termsMap.put(DwcTerm.vernacularName, v));

    return termsMap;
  }

  private Map<Term, String> toTermMap(ParsedName parsedName) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(parsedName.getInfraSpecificEpithet())
        .ifPresent(v -> termsMap.put(DwcTerm.infraspecificEpithet, v));

    Optional.ofNullable(parsedName.getSpecificEpithet())
        .ifPresent(v -> termsMap.put(DwcTerm.specificEpithet, v));

    Optional.ofNullable(parsedName.getInfraGeneric())
        .ifPresent(v -> termsMap.put(DwcTerm.infraspecificEpithet, v));

    Optional.ofNullable(parsedName.getType())
        .ifPresent(v -> termsMap.put(GbifTerm.nameType, v.name()));

    if (parsedName.isBinomial()) {
      Optional.ofNullable(parsedName.getGenusOrAbove())
          .ifPresent(v -> termsMap.put(GbifTerm.genericName, v));
    }

    return termsMap;
  }

  private String toString(Set<? extends Enum<?>> enums) {
    return enums.stream().map(Enum::name).collect(Collectors.joining(","));
  }
}
