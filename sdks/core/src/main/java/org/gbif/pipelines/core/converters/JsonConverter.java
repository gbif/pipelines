package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import com.google.common.base.Strings;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.AgentIdentifier;
import org.gbif.pipelines.io.avro.json.Authorship;
import org.gbif.pipelines.io.avro.json.Coordinates;
import org.gbif.pipelines.io.avro.json.Diagnostic;
import org.gbif.pipelines.io.avro.json.EventDate;
import org.gbif.pipelines.io.avro.json.GadmFeatures;
import org.gbif.pipelines.io.avro.json.GbifClassification;
import org.gbif.pipelines.io.avro.json.ParsedName;
import org.gbif.pipelines.io.avro.json.ParsedName.Builder;
import org.gbif.pipelines.io.avro.json.RankedName;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;
import org.gbif.pipelines.io.avro.json.VocabularyConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonConverter {

  private static final Set<String> EXCLUDE_ALL =
      Collections.singleton(DwcTerm.footprintWKT.qualifiedName());

  private static final Set<String> INCLUDE_EXT_ALL =
      new HashSet<>(
          Arrays.asList(
              Extension.MULTIMEDIA.getRowType(),
              Extension.AUDUBON.getRowType(),
              Extension.IMAGE.getRowType()));

  private static final String OCCURRENCE_EXT = "http://rs.tdwg.org/dwc/terms/Occurrence";

  private static final Map<Character, Character> CHAR_MAP = new HashMap<>(2);

  static {
    CHAR_MAP.put('\u001E', ',');
    CHAR_MAP.put('\u001f', ' ');
  }

  private static final LongFunction<LocalDateTime> DATE_FN =
      l -> LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));

  protected static String getEscapedText(String value) {
    String v = value;
    for (Entry<Character, Character> rule : CHAR_MAP.entrySet()) {
      v = v.trim().replace(rule.getKey(), rule.getValue());
    }
    return v;
  }

  /** Gets the maximum/latest created date of all the records. */
  public static Optional<String> getMaxCreationDate(SpecificRecordBase... recordBases) {
    return Arrays.stream(recordBases)
        .filter(Objects::nonNull)
        .filter(r -> Objects.nonNull(r.getSchema().getField(Indexing.CREATED)))
        .map(r -> r.get(Indexing.CREATED))
        .filter(Objects::nonNull)
        .map(Long.class::cast)
        .max(Long::compareTo)
        .flatMap(JsonConverter::convertToDate);
  }

  public static Optional<String> convertToDate(Long epoch) {
    return Optional.ofNullable(epoch).map(DATE_FN::apply).map(LocalDateTime::toString);
  }

  public static List<String> convertFieldAll(ExtendedRecord extendedRecord) {
    return convertFieldAll(extendedRecord, true);
  }

  public static List<String> convertFieldAll(
      ExtendedRecord extendedRecord, boolean includeExtensions) {
    Set<String> result = new HashSet<>();

    extendedRecord.getCoreTerms().entrySet().stream()
        .filter(term -> !EXCLUDE_ALL.contains(term.getKey()))
        .filter(term -> term.getValue() != null)
        .map(Entry::getValue)
        .forEach(result::add);

    if (includeExtensions) {
      extendedRecord.getExtensions().entrySet().stream()
          .filter(kv -> INCLUDE_EXT_ALL.contains(kv.getKey()))
          .map(Entry::getValue)
          .filter(Objects::nonNull)
          .flatMap(Collection::stream)
          .flatMap(map -> map.values().stream())
          .filter(Objects::nonNull)
          .forEach(result::add);
    }

    return result.stream()
        .flatMap(v -> Stream.of(v.split(ModelUtils.DEFAULT_SEPARATOR)))
        .map(JsonConverter::getEscapedText)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  public static List<String> convertExtensions(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions().entrySet().stream()
        .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
        .map(Entry::getKey)
        .distinct()
        .collect(Collectors.toList());
  }

  public static VerbatimRecord convertVerbatimRecord(ExtendedRecord extendedRecord) {
    return convertVerbatimRecord(extendedRecord, Collections.emptyList());
  }

  protected static VerbatimRecord convertVerbatimRecord(
      ExtendedRecord extendedRecord, List<String> excludeExtensions) {
    return VerbatimRecord.newBuilder()
        .setCore(extendedRecord.getCoreTerms())
        .setCoreId(extendedRecord.getCoreId())
        .setExtensions(filterExtensions(extendedRecord.getExtensions(), excludeExtensions))
        .build();
  }

  public static VerbatimRecord convertVerbatimEventRecord(ExtendedRecord extendedRecord) {
    return convertVerbatimRecord(extendedRecord, Collections.singletonList(OCCURRENCE_EXT));
  }

  private static Map<String, List<Map<String, String>>> filterExtensions(
      Map<String, List<Map<String, String>>> exts, List<String> excludedExtensions) {
    return exts.entrySet().stream()
        .filter(e -> !excludedExtensions.contains(e.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public static Optional<String> convertToMultivalue(List<String> list) {
    return list != null && !list.isEmpty()
        ? Optional.of(getEscapedText(String.join("|", list)))
        : Optional.empty();
  }

  public static List<String> getEscapedList(List<String> list) {
    return list.stream().map(JsonConverter::getEscapedText).collect(Collectors.toList());
  }

  public static Optional<String> convertLicense(String license) {
    return Optional.ofNullable(license)
        .filter(l -> !l.equals(License.UNSPECIFIED.name()))
        .filter(l -> !l.equals(License.UNSUPPORTED.name()));
  }

  public static List<AgentIdentifier> convertAgentList(
      List<org.gbif.pipelines.io.avro.AgentIdentifier> list) {
    return list.stream()
        .map(x -> AgentIdentifier.newBuilder().setType(x.getType()).setValue(x.getValue()).build())
        .collect(Collectors.toList());
  }

  public static Optional<VocabularyConcept> convertVocabularyConcept(
      org.gbif.pipelines.io.avro.VocabularyConcept concepts) {
    if (concepts == null) {
      return Optional.empty();
    }
    return Optional.of(
        VocabularyConcept.newBuilder()
            .setConcept(concepts.getConcept())
            .setLineage(concepts.getLineage())
            .build());
  }

  protected static void mapIssues(
      List<Issues> records, Consumer<List<String>> issueFn, Consumer<List<String>> notIssueFn) {
    Set<String> issues =
        records.stream()
            .flatMap(x -> x.getIssues().getIssueList().stream())
            .collect(Collectors.toSet());
    issueFn.accept(new ArrayList<>(issues));

    Set<String> notIssues =
        Arrays.stream(OccurrenceIssue.values())
            .map(Enum::name)
            .filter(x -> !issues.contains(x))
            .collect(Collectors.toSet());
    notIssueFn.accept(new ArrayList<>(notIssues));
  }

  public static List<String> convertMultimediaType(MultimediaRecord multimediaRecord) {
    return multimediaRecord.getMultimediaItems().stream()
        .map(Multimedia::getType)
        .filter(type -> !Strings.isNullOrEmpty(type))
        .distinct()
        .collect(Collectors.toList());
  }

  public static List<String> convertMultimediaLicense(MultimediaRecord multimediaRecord) {
    return multimediaRecord.getMultimediaItems().stream()
        .map(Multimedia::getLicense)
        .filter(license -> !Strings.isNullOrEmpty(license))
        .distinct()
        .collect(Collectors.toList());
  }

  public static List<org.gbif.pipelines.io.avro.json.Multimedia> convertMultimediaList(
      MultimediaRecord multimediaRecord) {
    return multimediaRecord.getMultimediaItems().stream()
        .map(
            m ->
                org.gbif.pipelines.io.avro.json.Multimedia.newBuilder()
                    .setType(m.getType())
                    .setFormat(m.getFormat())
                    .setIdentifier(m.getIdentifier())
                    .setAudience(m.getAudience())
                    .setContributor(m.getContributor())
                    .setCreated(m.getCreated())
                    .setCreator(m.getCreator())
                    .setDescription(m.getDescription())
                    .setLicense(m.getLicense())
                    .setPublisher(m.getPublisher())
                    .setReferences(m.getReferences())
                    .setRightsHolder(m.getRightsHolder())
                    .setSource(m.getSource())
                    .setTitle(m.getTitle())
                    .setDatasetId(m.getDatasetId())
                    .build())
        .collect(Collectors.toList());
  }

  public static Optional<EventDate> convertEventDate(
      org.gbif.pipelines.io.avro.EventDate eventDate) {
    return Optional.ofNullable(eventDate)
        .map(ed -> EventDate.newBuilder().setGte(ed.getGte()).setLte(ed.getLte()).build());
  }

  public static Optional<String> convertEventDateSingle(TemporalRecord temporalRecord) {
    Optional<TemporalAccessor> tao;
    if (temporalRecord.getEventDate() != null && temporalRecord.getEventDate().getGte() != null) {
      tao =
          Optional.ofNullable(temporalRecord.getEventDate().getGte())
              .map(StringToDateFunctions.getStringToTemporalAccessor());
    } else {
      tao =
          TemporalConverter.from(
              temporalRecord.getYear(), temporalRecord.getMonth(), temporalRecord.getDay());
    }
    return tao.map(ta -> TemporalAccessorUtils.toEarliestLocalDateTime(ta, true))
        .map(LocalDateTime::toString);
  }

  public static String convertScoordinates(Double lon, Double lat) {
    return "POINT (" + lon + " " + lat + ")";
  }

  public static Coordinates convertCoordinates(Double lon, Double lat) {
    return Coordinates.newBuilder().setLat(lat).setLon(lon).build();
  }

  /** All GADM GIDs as an array, for searching at multiple levels. */
  public static Optional<GadmFeatures> convertGadm(org.gbif.pipelines.io.avro.GadmFeatures gadm) {

    if (gadm == null) {
      return Optional.empty();
    }

    List<String> gids = new ArrayList<>(4);
    Optional.ofNullable(gadm.getLevel0Gid()).ifPresent(gids::add);
    Optional.ofNullable(gadm.getLevel1Gid()).ifPresent(gids::add);
    Optional.ofNullable(gadm.getLevel2Gid()).ifPresent(gids::add);
    Optional.ofNullable(gadm.getLevel3Gid()).ifPresent(gids::add);

    GadmFeatures gadmFeatures =
        GadmFeatures.newBuilder()
            .setLevel0Gid(gadm.getLevel0Gid())
            .setLevel0Name(gadm.getLevel0Name())
            .setLevel1Gid(gadm.getLevel1Gid())
            .setLevel1Name(gadm.getLevel1Name())
            .setLevel2Gid(gadm.getLevel2Gid())
            .setLevel2Name(gadm.getLevel2Name())
            .setLevel3Gid(gadm.getLevel3Gid())
            .setLevel3Name(gadm.getLevel3Name())
            .setGids(gids)
            .build();

    return Optional.of(gadmFeatures);
  }

  public static Optional<RankedName> convertRankedName(
      org.gbif.pipelines.io.avro.RankedName rankedName) {
    return Optional.ofNullable(rankedName)
        .map(
            rn ->
                RankedName.newBuilder()
                    .setName(rn.getName())
                    .setRank(rn.getRank() != null ? rn.getRank().name() : null)
                    .setKey(rn.getKey())
                    .build());
  }

  public static List<RankedName> convertRankedNames(
      List<org.gbif.pipelines.io.avro.RankedName> rankedNames) {
    return rankedNames.stream()
        .map(JsonConverter::convertRankedName)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  public static Optional<ParsedName> convertParsedName(
      org.gbif.pipelines.io.avro.ParsedName parsedName) {

    if (parsedName == null) {
      return Optional.empty();
    }

    Builder builder =
        ParsedName.newBuilder()
            .setAbbreviated(parsedName.getAbbreviated())
            .setAutonym(parsedName.getAutonym())
            .setBinomial(parsedName.getBinomial())
            .setCandidatus(parsedName.getCandidatus())
            .setCode(parsedName.getCode() != null ? parsedName.getCode().name() : null)
            .setDoubtful(parsedName.getDoubtful())
            .setGenus(parsedName.getGenus())
            .setIncomplete(parsedName.getIncomplete())
            .setIndetermined(parsedName.getIndetermined())
            .setInfraspecificEpithet(parsedName.getInfraspecificEpithet())
            .setNotho(parsedName.getNotho() != null ? parsedName.getNotho().name() : null)
            .setRank(parsedName.getRank() != null ? parsedName.getRank().name() : null)
            .setSpecificEpithet(parsedName.getSpecificEpithet())
            .setState(parsedName.getState() != null ? parsedName.getState().name() : null)
            .setTerminalEpithet(parsedName.getTerminalEpithet())
            .setTrinomial(parsedName.getTrinomial())
            .setType(parsedName.getType() != null ? parsedName.getType().name() : null)
            .setUninomial(parsedName.getUninomial());

    convertAuthorship(parsedName.getBasionymAuthorship()).ifPresent(builder::setBasionymAuthorship);
    convertAuthorship(parsedName.getCombinationAuthorship())
        .ifPresent(builder::setCombinationAuthorship);

    return Optional.of(builder.build());
  }

  public static Optional<Authorship> convertAuthorship(
      org.gbif.pipelines.io.avro.Authorship authorship) {
    return Optional.ofNullable(authorship)
        .map(
            a ->
                Authorship.newBuilder()
                    .setAuthors(a.getAuthors())
                    .setExAuthors(a.getAuthors())
                    .setEmpty(a.getEmpty())
                    .setYear(a.getYear())
                    .build());
  }

  public static Optional<Diagnostic> convertDiagnostic(
      org.gbif.pipelines.io.avro.Diagnostic diagnostic) {
    if (diagnostic == null) {
      return Optional.empty();
    }

    Diagnostic build =
        Diagnostic.newBuilder()
            .setMatchType(
                diagnostic.getMatchType() != null ? diagnostic.getMatchType().name() : null)
            .setNote(diagnostic.getNote())
            .setStatus(diagnostic.getStatus() != null ? diagnostic.getStatus().name() : null)
            .build();

    return Optional.of(build);
  }

  public static Optional<String> convertGenericName(TaxonRecord taxonRecord) {
    return Optional.ofNullable(taxonRecord.getUsageParsedName())
        .map(upn -> upn.getGenus() != null ? upn.getGenus() : upn.getUninomial());
  }

  public static GbifClassification convertClassification(
      ExtendedRecord verbatim, TaxonRecord taxon) {
    GbifClassification.Builder classificationBuilder =
        GbifClassification.newBuilder()
            .setSynonym(taxon.getSynonym())
            .setIucnRedListCategoryCode(taxon.getIucnRedListCategoryCode())
            .setClassification(JsonConverter.convertRankedNames(taxon.getClassification()))
            .setTaxonKey(JsonConverter.convertTaxonKey(taxon));

    JsonConverter.convertRankedName(taxon.getUsage()).ifPresent(classificationBuilder::setUsage);

    JsonConverter.convertRankedName(taxon.getAcceptedUsage())
        .ifPresent(classificationBuilder::setAcceptedUsage);

    JsonConverter.convertDiagnostic(taxon.getDiagnostics())
        .ifPresent(classificationBuilder::setDiagnostics);

    JsonConverter.convertParsedName(taxon.getUsageParsedName())
        .ifPresent(classificationBuilder::setUsageParsedName);

    JsonConverter.convertGenericName(taxon)
        .ifPresent(
            genericName -> {
              if (classificationBuilder.getUsageParsedName() != null) {
                classificationBuilder.getUsageParsedName().setGenericName(genericName);
              }
            });

    JsonConverter.convertClassificationPath(taxon)
        .ifPresent(classificationBuilder::setClassificationPath);

    // Classification
    if (taxon.getClassification() != null) {
      for (org.gbif.pipelines.io.avro.RankedName rankedName : taxon.getClassification()) {
        Rank rank = rankedName.getRank();
        switch (rank) {
          case KINGDOM:
            classificationBuilder.setKingdom(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setKingdomKey);
            break;
          case PHYLUM:
            classificationBuilder.setPhylum(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setPhylumKey);
            break;
          case CLASS:
            classificationBuilder.setClass$(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setClassKey);
            break;
          case ORDER:
            classificationBuilder.setOrder(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setOrderKey);
            break;
          case FAMILY:
            classificationBuilder.setFamily(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setFamilyKey);
            break;
          case GENUS:
            classificationBuilder.setGenus(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setGenusKey);
            break;
          case SPECIES:
            classificationBuilder.setSpecies(rankedName.getName());
            Optional.ofNullable(rankedName.getKey())
                .map(String::valueOf)
                .ifPresent(classificationBuilder::setSpeciesKey);
            break;
          default:
            // NOP
        }
      }
    }

    // Raw to index classification
    extractOptValue(verbatim, DwcTerm.taxonID).ifPresent(classificationBuilder::setTaxonID);
    extractOptValue(verbatim, DwcTerm.scientificName)
        .ifPresent(classificationBuilder::setVerbatimScientificName);

    return classificationBuilder.build();
  }
  /**
   * Creates a set of fields" kingdomKey, phylumKey, classKey, etc for convenient aggregation/facets
   */
  public static Optional<String> convertClassificationPath(TaxonRecord taxonRecord) {
    if (taxonRecord.getClassification() == null
        || taxonRecord.getClassification().isEmpty()
        || taxonRecord.getUsage() == null) {
      return Optional.empty();
    }

    String pathJoiner =
        taxonRecord.getClassification().stream()
            .filter(rankedName -> taxonRecord.getUsage().getRank() != rankedName.getRank())
            .map(rankedName -> rankedName.getKey().toString())
            .collect(Collectors.joining("_"));

    return Optional.of("_" + pathJoiner);
  }

  public static List<String> convertTaxonKey(TaxonRecord taxonRecord) {
    if (taxonRecord.getClassification() == null || taxonRecord.getClassification().isEmpty()) {
      return Collections.emptyList();
    }

    Set<Integer> taxonKey = new HashSet<>();

    Optional.ofNullable(taxonRecord.getUsage()).ifPresent(s -> taxonKey.add(s.getKey()));
    Optional.ofNullable(taxonRecord.getAcceptedUsage()).ifPresent(au -> taxonKey.add(au.getKey()));

    taxonRecord.getClassification().stream()
        .map(org.gbif.pipelines.io.avro.RankedName::getKey)
        .forEach(taxonKey::add);

    return taxonKey.stream().map(String::valueOf).collect(Collectors.toList());
  }
}
