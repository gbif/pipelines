package org.gbif.validator.api;

import java.net.URI;
import java.time.Year;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.AlternativeNames;
import org.gbif.dwc.terms.Term;

/**
 * NOTE: copied from COL to match Java version.
 *
 * <p>CoL terms covering all columns needed for the new CoL Data Package submission format:
 * https://github.com/CatalogueOfLife/datapackage-specs
 *
 * <p>To avoid dependency and clashes with DwC no terms are reused.
 */
public enum ColdpTerm implements Term, AlternativeNames {
  Reference((Class) null),
  ID,
  alternativeID,
  sourceID,
  citation,
  type(Enum.class),
  author,
  authorID,
  editor,
  editorID,
  title,
  titleShort("shortTitle"),
  containerAuthor,
  containerAuthorID,
  containerTitle("source"),
  containerTitleShort,
  issued(Date.class, "year"),
  accessed(Date.class),
  collectionTitle,
  collectionEditor,
  collectionEditorID,
  volume,
  issue,
  edition,
  page("details"),
  publisher,
  publisherPlace,
  version,
  isbn,
  issn,
  doi,
  link(URI.class),
  remarks,
  modified(Date.class),
  modifiedBy,

  Name((Class) null),
  // ID,
  // alternativeID
  // sourceID,
  basionymID("originalNameID"),
  scientificName,
  authorship,
  rank(Enum.class),
  uninomial,
  genus,
  infragenericEpithet,
  specificEpithet,
  infraspecificEpithet,
  cultivarEpithet,
  notho(Enum.class),
  originalSpelling(Boolean.class),
  combinationAuthorship,
  combinationAuthorshipID,
  combinationExAuthorship,
  combinationExAuthorshipID,
  combinationAuthorshipYear,
  basionymAuthorship,
  basionymAuthorshipID,
  basionymExAuthorship,
  basionymExAuthorshipID,
  basionymAuthorshipYear,
  code(Enum.class),
  status(Enum.class),
  referenceID("publishedInID"),
  publishedInYear(Year.class),
  publishedInPage,
  publishedInPageLink,
  gender(Enum.class),
  genderAgreement(Boolean.class),
  etymology,
  // link,
  // remarks,
  // modified(Date.class),
  // modifiedBy,

  Taxon((Class) null),
  // ID,
  // alternativeID
  // sourceID,
  parentID,
  ordinal(Integer.class, "sequenceIndex"),
  branchLength(Double.class),
  nameID,
  namePhrase,
  accordingToID,
  accordingToPage,
  accordingToPageLink,
  scrutinizer,
  scrutinizerID,
  scrutinizerDate(Date.class),
  provisional(Boolean.class),
  // referenceID,
  extinct(Boolean.class),
  temporalRangeStart,
  temporalRangeEnd,
  environment(Enum.class, "lifezone"),
  species,
  section,
  subgenus,
  // genus,
  subtribe,
  tribe,
  subfamily,
  family,
  superfamily,
  suborder,
  order,
  subclass,
  class_,
  subphylum,
  phylum,
  kingdom,
  // link,
  // remarks,
  // modified(Date.class),
  // modifiedBy,

  Synonym((Class) null),
  // ID,
  // sourceID,
  taxonID,
  // nameID,
  // namePhrase,
  // accordingToID,
  // accordingToPage,
  // accordingToPageLink,
  // status,
  // referenceID,
  // link,
  // remarks,
  // modified(Date.class),
  // modifiedBy,

  NameUsage((Class) null),
  // all of name, taxon & synonym - with the following additions due to naming clashes
  nameAlternativeID, // alternative term to Name.nameAlternativeID
  genericName, // alternative term to Name.genus
  nameReferenceID("namePublishedInID"), // alternative term to Name.referenceID
  namePublishedInYear(Year.class),
  namePublishedInPage,
  namePublishedInPageLink,
  nameStatus(Enum.class), // alternative term to Name.status
  nameRemarks, // alternative term to Name.remarks

  Author((Class) null),
  // ID,
  // sourceID,
  // alternativeID,
  given,
  // family,
  suffix,
  abbreviationBotany,
  alternativeNames,
  sex(Enum.class),
  country(Enum.class),
  birth,
  birthPlace,
  death,
  interest,
  affiliation,
  // referenceID,
  // link,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  NameRelation((Class) null, "NameRel"),
  // nameID,
  relatedNameID,
  // sourceID,
  // type,
  // referenceID,
  // page,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  TypeMaterial((Class) null),
  // ID,
  // nameID,
  // sourceID,
  // citation,
  // status,
  // country(Enum.class),
  locality,
  latitude,
  longitude,
  altitude,
  // sex(Enum.class),
  institutionCode,
  catalogNumber,
  associatedSequences,
  host,
  date,
  collector,
  // referenceID,
  // page,
  // link,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  TaxonConceptRelation((Class) null, "TaxonRelation"),
  // taxonID,
  relatedTaxonID,
  // sourceID,
  // type,
  // referenceID,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  SpeciesInteraction((Class) null),
  // taxonID,
  // relatedTaxonID,
  // sourceID,
  relatedTaxonScientificName,
  // type,
  // referenceID,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  TaxonProperty((Class) null),
  // taxonID,
  // sourceID,
  property,
  value,
  // ordinal,
  // referenceID,
  // page,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  Treatment((Class) null),
  // taxonID,
  // sourceID,
  document,
  format,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  Distribution((Class) null),
  // taxonID,
  // sourceID,
  areaID,
  area,
  gazetteer(Enum.class),
  establishmentMeans,
  degreeOfEstablishment,
  pathway,
  threatStatus,
  year(Integer.class),
  season,
  lifeStage,
  // referenceID,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  Media((Class) null),
  // taxonID,
  // sourceID,
  url(URI.class),
  // type,
  // title,
  created(Date.class),
  creator,
  license(Enum.class),
  // link,
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  VernacularName((Class) null),
  // taxonID,
  // sourceID,
  name,
  transliteration,
  language(Enum.class),
  preferred(Boolean.class),
  // country,
  // sex(Enum.class),
  // referenceID
  // remarks,
  // created(Date.class),
  // createdBy,
  // modified(Date.class),
  // modifiedBy,

  SpeciesEstimate((Class) null),
  // taxonID,
  // sourceID,
  estimate(Integer.class),
// type,
// referenceID,
// remarks,
// created(Date.class),
// createdBy,
// modified(Date.class),
// modifiedBy
;

  public static final String VERSION = "1.2"; // ColDP version supported
  private static final Map<String, ColdpTerm> LOOKUP =
      Arrays.stream(values()).collect(Collectors.toMap(ColdpTerm::normalize, Function.identity()));

  /** List of all higher rank terms, ordered by rank and starting with kingdom. */
  public static final ColdpTerm[] DENORMALIZED_RANKS = {
    ColdpTerm.kingdom,
    phylum,
    subphylum,
    class_,
    subclass,
    order,
    suborder,
    superfamily,
    family,
    subfamily,
    tribe,
    subtribe,
    genus,
    subgenus,
    section,
    species
  };

  public static Map<ColdpTerm, List<ColdpTerm>> RESOURCES =
      Map.ofEntries(
          Map.entry(
              Reference,
              List.of(
                  ID,
                  alternativeID,
                  sourceID,
                  citation,
                  type,
                  author,
                  authorID,
                  editor,
                  editorID,
                  title,
                  titleShort,
                  containerAuthor,
                  containerAuthorID,
                  containerTitle,
                  containerTitleShort,
                  issued,
                  accessed,
                  collectionTitle,
                  collectionEditor,
                  collectionEditorID,
                  volume,
                  issue,
                  edition,
                  page,
                  publisher,
                  publisherPlace,
                  version,
                  isbn,
                  issn,
                  doi,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              Name,
              List.of(
                  ID,
                  alternativeID,
                  sourceID,
                  basionymID,
                  scientificName,
                  authorship,
                  rank,
                  uninomial,
                  genus,
                  infragenericEpithet,
                  specificEpithet,
                  infraspecificEpithet,
                  cultivarEpithet,
                  notho,
                  originalSpelling,
                  combinationAuthorship,
                  combinationAuthorshipID,
                  combinationExAuthorship,
                  combinationExAuthorshipID,
                  combinationAuthorshipYear,
                  basionymAuthorship,
                  basionymAuthorshipID,
                  basionymExAuthorship,
                  basionymExAuthorshipID,
                  basionymAuthorshipYear,
                  code,
                  status,
                  referenceID,
                  publishedInYear,
                  publishedInPage,
                  publishedInPageLink,
                  gender,
                  genderAgreement,
                  etymology,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              NameRelation,
              List.of(
                  nameID,
                  relatedNameID,
                  sourceID,
                  type,
                  referenceID,
                  page,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              TypeMaterial,
              List.of(
                  ID,
                  nameID,
                  sourceID,
                  citation,
                  status,
                  referenceID,
                  page,
                  country,
                  locality,
                  latitude,
                  longitude,
                  altitude,
                  sex,
                  host,
                  associatedSequences,
                  date,
                  collector,
                  institutionCode,
                  catalogNumber,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              Author,
              List.of(
                  ID,
                  sourceID,
                  alternativeID,
                  given,
                  family,
                  suffix,
                  abbreviationBotany,
                  alternativeNames,
                  sex,
                  country,
                  birth,
                  birthPlace,
                  death,
                  affiliation,
                  interest,
                  referenceID,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              Taxon,
              List.of(
                  ID,
                  alternativeID,
                  sourceID,
                  parentID,
                  nameID,
                  namePhrase,
                  accordingToID,
                  accordingToPage,
                  accordingToPageLink,
                  provisional,
                  scrutinizer,
                  scrutinizerID,
                  scrutinizerDate,
                  extinct,
                  temporalRangeStart,
                  temporalRangeEnd,
                  environment,
                  referenceID,
                  species,
                  section,
                  subgenus,
                  genus,
                  subtribe,
                  tribe,
                  subfamily,
                  family,
                  superfamily,
                  suborder,
                  order,
                  subclass,
                  class_,
                  subphylum,
                  phylum,
                  kingdom,
                  ordinal,
                  branchLength,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              Synonym,
              List.of(
                  ID,
                  sourceID,
                  taxonID,
                  nameID,
                  namePhrase,
                  accordingToID,
                  accordingToPage,
                  accordingToPageLink,
                  status,
                  referenceID,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              NameUsage,
              List.of(
                  ID,
                  alternativeID,
                  nameAlternativeID,
                  sourceID,
                  parentID,
                  basionymID,
                  status,
                  scientificName,
                  authorship,
                  rank,
                  notho,
                  originalSpelling,
                  uninomial,
                  genericName,
                  infragenericEpithet,
                  specificEpithet,
                  infraspecificEpithet,
                  cultivarEpithet,
                  combinationAuthorship,
                  combinationAuthorshipID,
                  combinationExAuthorship,
                  combinationExAuthorshipID,
                  combinationAuthorshipYear,
                  basionymAuthorship,
                  basionymAuthorshipID,
                  basionymExAuthorship,
                  basionymExAuthorshipID,
                  basionymAuthorshipYear,
                  namePhrase,
                  nameReferenceID,
                  namePublishedInYear,
                  namePublishedInPage,
                  namePublishedInPageLink,
                  gender,
                  genderAgreement,
                  etymology,
                  code,
                  nameStatus,
                  accordingToID,
                  accordingToPage,
                  accordingToPageLink,
                  referenceID,
                  scrutinizer,
                  scrutinizerID,
                  scrutinizerDate,
                  extinct,
                  temporalRangeStart,
                  temporalRangeEnd,
                  environment,
                  species,
                  section,
                  subgenus,
                  genus,
                  subtribe,
                  tribe,
                  subfamily,
                  family,
                  superfamily,
                  suborder,
                  order,
                  subclass,
                  class_,
                  subphylum,
                  phylum,
                  kingdom,
                  ordinal,
                  branchLength,
                  link,
                  nameRemarks,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              SpeciesInteraction,
              List.of(
                  taxonID,
                  relatedTaxonID,
                  sourceID,
                  relatedTaxonScientificName,
                  type,
                  referenceID,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              TaxonConceptRelation,
              List.of(
                  taxonID,
                  relatedTaxonID,
                  sourceID,
                  type,
                  referenceID,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              TaxonProperty,
              List.of(
                  taxonID,
                  sourceID,
                  property,
                  value,
                  ordinal,
                  referenceID,
                  page,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(Treatment, List.of(taxonID, sourceID, document, format, modified, modifiedBy)),
          Map.entry(
              Distribution,
              List.of(
                  taxonID,
                  sourceID,
                  areaID,
                  area,
                  gazetteer,
                  establishmentMeans,
                  degreeOfEstablishment,
                  pathway,
                  threatStatus,
                  year,
                  season,
                  lifeStage,
                  referenceID,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              Media,
              List.of(
                  taxonID,
                  sourceID,
                  url,
                  type,
                  title,
                  created,
                  creator,
                  license,
                  link,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              VernacularName,
              List.of(
                  taxonID,
                  sourceID,
                  name,
                  transliteration,
                  language,
                  preferred,
                  country,
                  area,
                  sex,
                  referenceID,
                  remarks,
                  modified,
                  modifiedBy)),
          Map.entry(
              SpeciesEstimate,
              List.of(
                  taxonID, sourceID, estimate, type, referenceID, remarks, modified, modifiedBy)));

  private static final String PREFIX = "col";
  private static final String NS = "https://terms.catalogueoflife.org/";
  private static final URI NS_URI = URI.create(NS);

  private final Class<?> dataType;
  private final String[] alternatives;

  ColdpTerm() {
    this(String.class);
  }

  ColdpTerm(String... alternatives) {
    this.dataType = String.class;
    this.alternatives = alternatives;
  }

  ColdpTerm(Class<?> type) {
    this.dataType = type;
    this.alternatives = new String[0];
  }

  ColdpTerm(Class<?> type, String... alternatives) {
    this.dataType = type;
    this.alternatives = alternatives;
  }

  @Override
  public String prefix() {
    return PREFIX;
  }

  @Override
  public URI namespace() {
    return NS_URI;
  }

  @Override
  public String simpleName() {
    if (this == class_) {
      return "class";
    }
    return name();
  }

  @Override
  public String toString() {
    return prefixedName();
  }

  @Override
  public String[] alternativeNames() {
    return this.alternatives;
  }

  @Override
  public boolean isClass() {
    return dataType == null;
  }

  public Class<?> getType() {
    return dataType;
  }

  private static String normalize(String x, boolean isClass) {
    x = x.replaceAll("[-_ ]+", "").toLowerCase();
    return isClass ? Character.toUpperCase(x.charAt(0)) + x.substring(1) : x;
  }

  private static String normalize(ColdpTerm t) {
    return normalize(t.name(), t.isClass());
  }

  public static ColdpTerm find(String name, boolean isClass) {
    return LOOKUP.getOrDefault(normalize(name, isClass), null);
  }
}
