package org.gbif.pipelines.validator.checklists.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.gbif.api.model.checklistbank.Description;
import org.gbif.api.model.checklistbank.Distribution;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMediaObject;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.checklistbank.Reference;
import org.gbif.api.model.checklistbank.SpeciesProfile;
import org.gbif.api.model.checklistbank.TypeSpecimen;
import org.gbif.api.model.checklistbank.VernacularName;
import org.gbif.api.model.common.Identifier;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;

@UtilityClass
public class ObjectToTermMapper {

  /** Converts a NameUsage to a Map<Term,String>. */
  public static Map<Term, String> toTermMap(NameUsage nameUsage) {
    Map<Term, String> termsMap = new HashMap<>();
    if (nameUsage != null) {
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
          .filter(s -> !s.isEmpty())
          .ifPresent(v -> termsMap.put(DwcTerm.nomenclaturalStatus, toString(v)));

      Optional.ofNullable(nameUsage.getKingdom()).ifPresent(v -> termsMap.put(DwcTerm.kingdom, v));

      Optional.ofNullable(nameUsage.getPhylum()).ifPresent(v -> termsMap.put(DwcTerm.phylum, v));

      Optional.ofNullable(nameUsage.getClazz()).ifPresent(v -> termsMap.put(DwcTerm.class_, v));

      Optional.ofNullable(nameUsage.getOrder()).ifPresent(v -> termsMap.put(DwcTerm.order, v));

      Optional.ofNullable(nameUsage.getFamily()).ifPresent(v -> termsMap.put(DwcTerm.family, v));

      Optional.ofNullable(nameUsage.getGenus()).ifPresent(v -> termsMap.put(DwcTerm.genus, v));

      Optional.ofNullable(nameUsage.getSubgenus())
          .ifPresent(v -> termsMap.put(DwcTerm.subgenus, v));

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
    }
    return termsMap;
  }

  public static List<Map<Term, String>> toNameUsageTermMap(List<NameUsage> nameUsages) {
    return nameUsages.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(ParsedName parsedName) {
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
          .ifPresent(v -> termsMap.put(DwcTerm.genericName, v));
    }

    return termsMap;
  }

  public static Map<Term, String> toTermMap(Description description) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(description.getDescription())
        .ifPresent(v -> termsMap.put(DcTerm.description, v));

    Optional.ofNullable(description.getType()).ifPresent(v -> termsMap.put(DcTerm.type, v));

    Optional.ofNullable(description.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(description.getLanguage())
        .ifPresent(v -> termsMap.put(DcTerm.language, v.getIso2LetterCode()));

    Optional.ofNullable(description.getCreator()).ifPresent(v -> termsMap.put(DcTerm.creator, v));

    Optional.ofNullable(description.getContributor())
        .ifPresent(v -> termsMap.put(DcTerm.contributor, v));

    Optional.ofNullable(description.getLicense()).ifPresent(v -> termsMap.put(DcTerm.license, v));

    Optional.ofNullable(description.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    // Terms not mapped datasetID, created, rightsHolder and audience

    return termsMap;
  }

  public static List<Map<Term, String>> toDescriptionTermMap(List<Description> descriptions) {
    return descriptions.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(Distribution distribution) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(distribution.getLocationId())
        .ifPresent(v -> termsMap.put(DwcTerm.locationID, v));

    Optional.ofNullable(distribution.getLocality())
        .ifPresent(v -> termsMap.put(DwcTerm.locality, v));

    Optional.ofNullable(distribution.getCountry())
        .ifPresent(v -> termsMap.put(DwcTerm.countryCode, v.getIso2LetterCode()));

    Optional.ofNullable(distribution.getLifeStage())
        .ifPresent(v -> termsMap.put(DwcTerm.lifeStage, v.name()));

    Optional.ofNullable(distribution.getThreatStatus())
        .ifPresent(v -> termsMap.put(IucnTerm.iucnRedListCategory, v.name()));

    Optional.ofNullable(distribution.getStatus())
        .ifPresent(v -> termsMap.put(DwcTerm.occurrenceStatus, v.name()));

    Optional.ofNullable(distribution.getEstablishmentMeans())
        .ifPresent(v -> termsMap.put(DwcTerm.establishmentMeans, v.name()));

    Optional.ofNullable(distribution.getAppendixCites())
        .ifPresent(v -> termsMap.put(GbifTerm.appendixCITES, v.name()));

    Optional.ofNullable(distribution.getStartDayOfYear())
        .ifPresent(v -> termsMap.put(DwcTerm.startDayOfYear, v.toString()));

    Optional.ofNullable(distribution.getEndDayOfYear())
        .ifPresent(v -> termsMap.put(DwcTerm.endDayOfYear, v.toString()));

    Optional.ofNullable(distribution.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(distribution.getRemarks())
        .ifPresent(v -> termsMap.put(DwcTerm.occurrenceRemarks, v));

    // not mapped distribution.getTemporal()

    // Terms not mapped datasetID and eventDate

    return termsMap;
  }

  public static List<Map<Term, String>> toDistributionTermMap(List<Distribution> distributions) {
    return distributions.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(Identifier identifier) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(identifier.getIdentifier())
        .ifPresent(v -> termsMap.put(DcTerm.identifier, v));

    Optional.ofNullable(identifier.getTitle()).ifPresent(v -> termsMap.put(DcTerm.title, v));

    // Terms not mapped subject, datasetID and format
    // Fields not mapped identifier.getType

    return termsMap;
  }

  public static List<Map<Term, String>> toIdentifierTermMap(List<Identifier> identifiers) {
    return identifiers.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(NameUsageMediaObject nameUsageMediaObject) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(nameUsageMediaObject.getType())
        .ifPresent(v -> termsMap.put(DcTerm.type, v.name()));

    Optional.ofNullable(nameUsageMediaObject.getFormat())
        .ifPresent(v -> termsMap.put(DcTerm.format, v));

    Optional.ofNullable(nameUsageMediaObject.getIdentifier())
        .ifPresent(v -> termsMap.put(DcTerm.identifier, v.toString()));

    Optional.ofNullable(nameUsageMediaObject.getReferences())
        .ifPresent(v -> termsMap.put(DcTerm.references, v.toString()));

    Optional.ofNullable(nameUsageMediaObject.getTitle())
        .ifPresent(v -> termsMap.put(DcTerm.title, v));

    Optional.ofNullable(nameUsageMediaObject.getDescription())
        .ifPresent(v -> termsMap.put(DcTerm.description, v));

    Optional.ofNullable(nameUsageMediaObject.getCreated())
        .ifPresent(v -> termsMap.put(DcTerm.created, v.toString()));

    Optional.ofNullable(nameUsageMediaObject.getCreator())
        .ifPresent(v -> termsMap.put(DcTerm.creator, v));

    Optional.ofNullable(nameUsageMediaObject.getContributor())
        .ifPresent(v -> termsMap.put(DcTerm.contributor, v));

    Optional.ofNullable(nameUsageMediaObject.getPublisher())
        .ifPresent(v -> termsMap.put(DcTerm.publisher, v));

    Optional.ofNullable(nameUsageMediaObject.getAudience())
        .ifPresent(v -> termsMap.put(DcTerm.audience, v));

    Optional.ofNullable(nameUsageMediaObject.getSource())
        .ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(nameUsageMediaObject.getLicense())
        .ifPresent(v -> termsMap.put(DcTerm.license, v));

    Optional.ofNullable(nameUsageMediaObject.getRightsHolder())
        .ifPresent(v -> termsMap.put(DcTerm.rightsHolder, v));

    // Terms not mapped  datasetID

    return termsMap;
  }

  public static List<Map<Term, String>> toNameUsageMediaObjectTermMap(
      List<NameUsageMediaObject> nameUsageMediaObjects) {
    return nameUsageMediaObjects.stream()
        .map(ObjectToTermMapper::toTermMap)
        .collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(Reference reference) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(reference.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(reference.getRemarks())
        .ifPresent(v -> termsMap.put(DwcTerm.taxonRemarks, v));

    Optional.ofNullable(reference.getType()).ifPresent(v -> termsMap.put(DcTerm.type, v));

    Optional.ofNullable(reference.getCitation())
        .ifPresent(v -> termsMap.put(DcTerm.bibliographicCitation, v));

    // Terms not mapped title, creator, date, description, subject, language, rights and datasetID
    // Fields not mapped reference.getDoi, reference.getLink

    return termsMap;
  }

  public static List<Map<Term, String>> toReferenceTermMap(List<Reference> references) {
    return references.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(SpeciesProfile speciesProfile) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(speciesProfile.isMarine())
        .ifPresent(v -> termsMap.put(GbifTerm.isMarine, v.toString()));

    Optional.ofNullable(speciesProfile.isFreshwater())
        .ifPresent(v -> termsMap.put(GbifTerm.isFreshwater, v.toString()));

    Optional.ofNullable(speciesProfile.isTerrestrial())
        .ifPresent(v -> termsMap.put(GbifTerm.isTerrestrial, v.toString()));

    Optional.ofNullable(speciesProfile.isExtinct())
        .ifPresent(v -> termsMap.put(GbifTerm.isExtinct, v.toString()));

    Optional.ofNullable(speciesProfile.isHybrid())
        .ifPresent(v -> termsMap.put(GbifTerm.isHybrid, v.toString()));

    Optional.ofNullable(speciesProfile.getLivingPeriod())
        .ifPresent(v -> termsMap.put(GbifTerm.livingPeriod, v));

    Optional.ofNullable(speciesProfile.getAgeInDays())
        .ifPresent(v -> termsMap.put(GbifTerm.ageInDays, v.toString()));

    Optional.ofNullable(speciesProfile.getSizeInMillimeter())
        .ifPresent(v -> termsMap.put(GbifTerm.sizeInMillimeter, v.toString()));

    Optional.ofNullable(speciesProfile.getMassInGram())
        .ifPresent(v -> termsMap.put(GbifTerm.massInGram, v.toString()));

    Optional.ofNullable(speciesProfile.getHabitat())
        .ifPresent(v -> termsMap.put(DwcTerm.habitat, v));

    // Terms not mapped isInvasive, sex and datasetID
    // Fields not mapped speciesProfile.getSource()

    return termsMap;
  }

  public static List<Map<Term, String>> toSpeciesProfileTermMap(
      List<SpeciesProfile> speciesProfiles) {
    return speciesProfiles.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  public static Map<Term, String> toTermMap(TypeSpecimen typeSpecimen) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(typeSpecimen.getTypeStatus())
        .ifPresent(v -> termsMap.put(DwcTerm.typeStatus, v.toString()));

    Optional.ofNullable(typeSpecimen.getTypeDesignationType())
        .ifPresent(v -> termsMap.put(GbifTerm.typeDesignationType, v.toString()));

    Optional.ofNullable(typeSpecimen.getTypeDesignatedBy())
        .ifPresent(v -> termsMap.put(GbifTerm.typeDesignatedBy, v));

    Optional.ofNullable(typeSpecimen.getScientificName())
        .ifPresent(v -> termsMap.put(DwcTerm.scientificName, v));

    Optional.ofNullable(typeSpecimen.getTaxonRank())
        .ifPresent(v -> termsMap.put(DwcTerm.taxonRank, v.name()));

    Optional.ofNullable(typeSpecimen.getCitation())
        .ifPresent(v -> termsMap.put(DcTerm.bibliographicCitation, v));

    Optional.ofNullable(typeSpecimen.getOccurrenceId())
        .ifPresent(v -> termsMap.put(DwcTerm.occurrenceID, v));

    Optional.ofNullable(typeSpecimen.getInstitutionCode())
        .ifPresent(v -> termsMap.put(DwcTerm.institutionCode, v));

    Optional.ofNullable(typeSpecimen.getCollectionCode())
        .ifPresent(v -> termsMap.put(DwcTerm.collectionCode, v));

    Optional.ofNullable(typeSpecimen.getCatalogNumber())
        .ifPresent(v -> termsMap.put(DwcTerm.catalogNumber, v));

    Optional.ofNullable(typeSpecimen.getLocality())
        .ifPresent(v -> termsMap.put(DwcTerm.locality, v));

    Optional.ofNullable(typeSpecimen.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(typeSpecimen.getVerbatimEventDate())
        .ifPresent(v -> termsMap.put(DwcTerm.verbatimEventDate, v));

    Optional.ofNullable(typeSpecimen.getVerbatimLabel())
        .ifPresent(v -> termsMap.put(DwcTerm.verbatimLabel, v));

    Optional.ofNullable(typeSpecimen.getVerbatimLatitude())
        .ifPresent(v -> termsMap.put(DwcTerm.verbatimLatitude, v));

    Optional.ofNullable(typeSpecimen.getVerbatimLongitude())
        .ifPresent(v -> termsMap.put(DwcTerm.verbatimLongitude, v));

    // Terms not mapped sex and datasetID

    return termsMap;
  }

  public static Map<Term, String> toTermMap(VernacularName vernacularName) {
    Map<Term, String> termsMap = new HashMap<>();

    Optional.ofNullable(vernacularName.getVernacularName())
        .ifPresent(v -> termsMap.put(DwcTerm.vernacularName, v));

    Optional.ofNullable(vernacularName.getSource()).ifPresent(v -> termsMap.put(DcTerm.source, v));

    Optional.ofNullable(vernacularName.getLanguage())
        .ifPresent(v -> termsMap.put(DcTerm.language, v.getIso2LetterCode()));

    Optional.ofNullable(vernacularName.getCountry())
        .ifPresent(v -> termsMap.put(DwcTerm.countryCode, v.getIso2LetterCode()));

    Optional.ofNullable(vernacularName.getSex())
        .ifPresent(v -> termsMap.put(DwcTerm.sex, v.name()));

    Optional.ofNullable(vernacularName.getLifeStage())
        .ifPresent(v -> termsMap.put(DwcTerm.lifeStage, v.name()));

    Optional.ofNullable(vernacularName.isPlural())
        .ifPresent(v -> termsMap.put(GbifTerm.isPlural, v.toString()));

    Optional.ofNullable(vernacularName.isPreferred())
        .ifPresent(v -> termsMap.put(GbifTerm.isPreferredName, v.toString()));

    Optional.ofNullable(vernacularName.getArea()).ifPresent(v -> termsMap.put(DwcTerm.locality, v));

    // Terms not mapped temporal, locationID, organismPart, taxonRemarks and datasetID

    return termsMap;
  }

  public static List<Map<Term, String>> toVernacularNameTermMap(
      List<VernacularName> vernacularNames) {
    return vernacularNames.stream().map(ObjectToTermMapper::toTermMap).collect(Collectors.toList());
  }

  private static String toString(Set<? extends Enum<?>> enums) {
    return enums.stream().map(Enum::name).collect(Collectors.joining(","));
  }
}
