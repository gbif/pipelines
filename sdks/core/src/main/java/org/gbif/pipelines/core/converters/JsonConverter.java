package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.converters.OccurrenceJsonConverter.GBIF_BACKBONE_DATASET_KEY;

import com.google.common.base.Strings;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.json.*;
import org.gbif.pipelines.io.avro.json.AgentIdentifier;
import org.gbif.pipelines.io.avro.json.Coordinates;
import org.gbif.pipelines.io.avro.json.EventDate;
import org.gbif.pipelines.io.avro.json.GadmFeatures;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;
import org.gbif.pipelines.io.avro.json.VocabularyConcept;
import org.gbif.pipelines.io.avro.json.VocabularyConceptList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonConverter {

  private static final Set<String> EXCLUDE_ALL =
      new HashSet<>(
          Arrays.asList(
              DwcTerm.footprintWKT.qualifiedName(),
              DwcTerm.previousIdentifications.qualifiedName()));

  private static final Set<String> INCLUDE_EXT_ALL =
      new HashSet<>(
          Arrays.asList(
              Extension.MULTIMEDIA.getRowType(),
              Extension.AUDUBON.getRowType(),
              Extension.IMAGE.getRowType()));

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
    return convertVerbatimRecord(
        extendedRecord, Collections.singletonList(ConverterConstants.OCCURRENCE_EXT));
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

  public static Optional<VocabularyConceptList> convertVocabularyConceptList(
      List<org.gbif.pipelines.io.avro.VocabularyConcept> concepts) {
    if (concepts == null || concepts.isEmpty()) {
      return Optional.empty();
    }

    List<String> allConcepts =
        concepts.stream()
            .map(org.gbif.pipelines.io.avro.VocabularyConcept::getConcept)
            .collect(Collectors.toList());

    List<String> allParents =
        concepts.stream().flatMap(c -> c.getLineage().stream()).collect(Collectors.toList());

    return Optional.of(
        VocabularyConceptList.newBuilder().setConcepts(allConcepts).setLineage(allParents).build());
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

  public static Optional<String> convertEventDateInterval(TemporalRecord temporalRecord) {
    if (temporalRecord.getEventDate() != null
        && temporalRecord.getEventDate().getInterval() != null) {
      return Optional.of(temporalRecord.getEventDate().getInterval());
    }
    return Optional.empty();
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

  private static Optional<Usage> buildUsage(
      org.gbif.pipelines.io.avro.TaxonRecord taxonRecord, boolean accepted) {
    if (taxonRecord == null) {
      return Optional.empty();
    }

    var usageData = accepted ? taxonRecord.getAcceptedUsage() : taxonRecord.getUsage();
    Usage.Builder builder = Usage.newBuilder();

    if (usageData != null) {
      builder
          .setName(usageData.getName())
          .setRank(usageData.getRank())
          .setKey(usageData.getKey())
          .setAuthorship(usageData.getAuthorship())
          .setCode(usageData.getCode())
          .setSpecificEpithet(usageData.getSpecificEpithet())
          .setInfragenericEpithet(usageData.getInfragenericEpithet())
          .setInfraspecificEpithet(usageData.getInfraspecificEpithet())
          .setGenericName(usageData.getGenericName())
          .setFormattedName(usageData.getFormattedName());
    }

    return Optional.of(builder.build());
  }

  public static Map<String, Classification> convertToClassifications(MultiTaxonRecord taxon) {
    return taxon.getTaxonRecords().stream()
        .filter(
            tr ->
                tr.getUsage() != null
                    && !TaxonomyInterpreter.INCERTAE_SEDIS_KEY.equals(tr.getUsage().getKey()))
        .collect(
            Collectors.toMap(TaxonRecord::getDatasetKey, JsonConverter::convertToClassification));
  }

  private static LinkedHashMap<String, String> convertToMap(
      List<org.gbif.pipelines.io.avro.RankedName> names,
      Function<org.gbif.pipelines.io.avro.RankedName, String> valueExtractor) {

    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    Set<String> ranks = new LinkedHashSet<>();
    int depth = 0;
    for (org.gbif.pipelines.io.avro.RankedName rankedName : names) {
      String rankToUse = rankedName.getRank();
      if (ranks.contains(rankedName.getRank())) {
        rankToUse = rankedName.getRank() + "_" + depth;
      } else {
        ranks.add(rankedName.getRank());
      }
      map.put(rankToUse, valueExtractor.apply(rankedName));
      depth++;
    }
    return map;
  }

  public static Classification convertToClassification(TaxonRecord taxon) {

    Classification.Builder classificationBuilder =
        Classification.newBuilder()
            .setClassification(
                convertToMap(
                    taxon.getClassification(), org.gbif.pipelines.io.avro.RankedName::getName))
            .setClassificationKeys(
                convertToMap(
                    taxon.getClassification(), org.gbif.pipelines.io.avro.RankedName::getKey))
            .setTaxonKeys(JsonConverter.convertTaxonKey(taxon))
            .setIucnRedListCategoryCode(taxon.getIucnRedListCategoryCode())
            .setUsage(JsonConverter.buildUsage(taxon, false).orElse(null))
            .setStatus(taxon.getUsage() != null ? taxon.getUsage().getStatus() : null)
            .setAcceptedUsage(JsonConverter.buildUsage(taxon, true).orElse(null));

    if (taxon.getIssues() != null
        && taxon.getIssues().getIssueList() != null
        && !taxon.getIssues().getIssueList().isEmpty()) {
      classificationBuilder.setIssues(taxon.getIssues().getIssueList());
    } else {
      classificationBuilder.setIssues(Collections.emptyList());
    }

    JsonConverter.convertClassificationDepth(taxon)
        .ifPresent(classificationBuilder::setClassificationDepth);

    return classificationBuilder.build();
  }

  public static Classification convertToClassificationFromMultiTaxon(
      ExtendedRecord verbatim, MultiTaxonRecord multiTaxon) {
    if (multiTaxon != null
        && multiTaxon.getTaxonRecords() != null
        && !multiTaxon.getTaxonRecords().isEmpty()) {

      Optional<TaxonRecord> gbifRecord =
          multiTaxon.getTaxonRecords().stream()
              .filter(tr -> GBIF_BACKBONE_DATASET_KEY.equals(tr.getDatasetKey()))
              .findFirst();

      return gbifRecord.map(JsonConverter::convertToClassification).orElse(null);
    }
    return null;
  }

  /**
   * Creates a set of fields" kingdomKey, phylumKey, classKey, etc for convenient aggregation/facets
   */
  public static Optional<Map<String, String>> convertClassificationDepth(TaxonRecord taxonRecord) {
    if (taxonRecord.getClassification() == null
        || taxonRecord.getClassification().isEmpty()
        || taxonRecord.getUsage() == null) {
      return Optional.empty();
    }

    Map<String, String> depthMap = new LinkedHashMap<>();
    AtomicInteger idx = new AtomicInteger(0); // Using AtomicInteger to handle index
    taxonRecord
        .getClassification()
        .forEach(taxon -> depthMap.put(String.valueOf(idx.getAndIncrement()), taxon.getKey()));
    return Optional.of(depthMap);
  }

  public static List<String> convertTaxonKey(TaxonRecord taxonRecord) {
    if (taxonRecord.getClassification() == null || taxonRecord.getClassification().isEmpty()) {
      return Collections.emptyList();
    }

    Set<String> taxonKey = new LinkedHashSet<>();

    taxonRecord.getClassification().stream()
        .map(org.gbif.pipelines.io.avro.RankedName::getKey)
        .forEach(taxonKey::add);

    Optional.ofNullable(taxonRecord.getUsage()).ifPresent(s -> taxonKey.add(s.getKey()));
    Optional.ofNullable(taxonRecord.getAcceptedUsage()).ifPresent(au -> taxonKey.add(au.getKey()));

    return taxonKey.stream().map(String::valueOf).collect(Collectors.toList());
  }
}
