package org.gbif.pipelines.core.converters;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.utils.TemporalUtils;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.BlastResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.core.converters.JsonConverter.getEscapedTextNode;

/**
 * Converter for objects to GBIF elasticsearch schema. You can pass any {@link SpecificRecordBase} objects(Avro
 * generated)
 *
 * <pre>{@code
 * Usage example:
 *
 * BasicRecord br = ...
 * TemporalRecord tmr =  ...
 * LocationRecord lr =  ...
 * TaxonRecord tr =  ...
 * MultimediaRecord mr =  ...
 * ExtendedRecord er =  ...
 * String result = GbifJsonConverter.toStringJson(br, tmr, lr, tr, mr, er);
 *
 * }</pre>
 */
@SuppressWarnings("FallThrough")
@Slf4j
@Builder
public class GbifJsonConverter {

  private static final String ID = "id";
  private static final String ISSUES = "issues";
  private static final String CREATED_FIELD = "created";

  private static final Set<String> EXCLUDE_ALL = Collections.singleton(DwcTerm.footprintWKT.qualifiedName());

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final LongFunction<LocalDateTime> DATE_FN =
      l -> LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));

  private final JsonConverter.JsonConverterBuilder builder =
      JsonConverter.builder()
          .skipKey("decimalLatitude")
          .skipKey("decimalLongitude")
          .skipKey("machineTags")
          .skipKey(CREATED_FIELD)
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(LocationRecord.class, getLocationRecordConverter())
          .converter(TemporalRecord.class, getTemporalRecordConverter())
          .converter(TaxonRecord.class, getTaxonomyRecordConverter())
          .converter(LocationFeatureRecord.class, getLocationFeatureRecordConverter())
          .converter(AmplificationRecord.class, getAmplificationRecordConverter())
          .converter(MeasurementOrFactRecord.class, getMeasurementOrFactRecordConverter())
          .converter(MultimediaRecord.class, getMultimediaConverter())
          .converter(TaggedValueRecord.class, getTaggedValueConverter())
          .converter(BasicRecord.class, getBasicRecordConverter());

  @Builder.Default
  private boolean skipIssues = false;

  @Builder.Default
  private boolean skipId = true;

  @Singular
  private List<SpecificRecordBase> records;

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to the new ES
   * record
   */
  public static ObjectNode toJson(SpecificRecordBase... records) {
    return GbifJsonConverter.builder()
        .records(Arrays.asList(records))
        .build()
        .toJson();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to a partial ES
   * record update
   */
  public static ObjectNode toPartialJson(SpecificRecordBase... records) {
    return GbifJsonConverter.builder()
        .records(Arrays.asList(records))
        .skipId(false)
        .skipIssues(true)
        .build()
        .toJson();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object, suited to the new ES
   * record
   */
  public static String toStringJson(SpecificRecordBase... records) {
    return toJson(records).toString();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object, suited to a partial ES
   * record update
   */
  public static String toStringPartialJson(SpecificRecordBase... records) {
    return toPartialJson(records).toString();
  }

  /** Change the json result, merging all issues from records to one array */
  public ObjectNode toJson() {
    builder.records(records);
    if (skipId) {
      builder.skipKey(ID);
    }
    if (skipIssues) {
      builder.skipKey(ISSUES);
    }

    ObjectNode mainNode = builder.build().toJson();

    if (!skipIssues) {
      addIssues(mainNode);
    }

    getMaxCreationDate(mainNode).ifPresent(
        createdDate -> mainNode.set(CREATED_FIELD, new TextNode(createdDate.toString())));

    Optional.ofNullable(mainNode.get("lastCrawled")).ifPresent(
        lastCrawled -> mainNode.set("lastCrawled", new TextNode(DATE_FN.apply(lastCrawled.asLong()).toString()))
    );

    return mainNode;
  }

  /**
   * Gets the maximum/latest created date of all the records.
   */
  private Optional<LocalDateTime> getMaxCreationDate(ObjectNode rootNode) {
    return Optional.ofNullable(rootNode.get(CREATED_FIELD))
        .map(created -> Optional.of(DATE_FN.apply(rootNode.get(CREATED_FIELD).asLong())))
        .orElseGet(() -> records.stream()
            .filter(record -> Objects.nonNull(record.getSchema().getField(CREATED_FIELD)))
            .map(record -> record.get(CREATED_FIELD))
            .filter(Objects::nonNull)
            .map(x -> DATE_FN.apply((Long) x))
            .max(LocalDateTime::compareTo));
  }

  @Override
  public String toString() {
    return toJson().toString();
  }

  /**
   * Adds issues and  notIssues json nodes
   */
  private void addIssues(ObjectNode mainNode) {
    // Issues
    Set<String> issues = records.stream()
        .filter(Issues.class::isInstance)
        .flatMap(x -> ((Issues) x).getIssues().getIssueList().stream())
        .collect(Collectors.toSet());
    ArrayNode issueArrayNodes = JsonConverter.createArrayNode();
    issues.forEach(issueArrayNodes::add);
    mainNode.set(ISSUES, issueArrayNodes);

    // Not issues
    Set<String> notIssues = Arrays.stream(OccurrenceIssue.values())
        .map(Enum::name)
        .filter(x -> !issues.contains(x))
        .collect(Collectors.toSet());

    ArrayNode arrayNotIssuesNode = JsonConverter.createArrayNode();
    notIssues.forEach(arrayNotIssuesNode::add);
    mainNode.set("notIssues", arrayNotIssuesNode);
  }

  /**
   * String converter for {@link ExtendedRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "verbatim": {
   *    "core":{
   *      "continent": "North America",
   *      //.....more fields
   *    }
   *    "extensions": {
   *      ...
   *    }
   * },
   * "basisOfRecord": null,
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getExtendedRecordConverter() {
    return (jc, record) -> {

      ExtendedRecord er = (ExtendedRecord) record;

      jc.addJsonTextFieldNoCheck(ID, er.getId());

      Map<String, String> core = er.getCoreTerms();
      Map<String, List<Map<String, String>>> ext = er.getExtensions();

      BiConsumer<Term, String> fieldFn = (t, k) ->
          Optional.ofNullable(core.get(t.qualifiedName())).ifPresent(r -> jc.addJsonTextFieldNoCheck(k, r));

      fieldFn.accept(DwcTerm.recordedBy, "recordedBy");
      fieldFn.accept(DwcTerm.identifiedBy, "identifiedBy");
      fieldFn.accept(DwcTerm.recordNumber, "recordNumber");
      fieldFn.accept(DwcTerm.organismID, "organismId");
      fieldFn.accept(DwcTerm.samplingProtocol, "samplingProtocol");
      fieldFn.accept(DwcTerm.eventID, "eventId");
      fieldFn.accept(DwcTerm.parentEventID, "parentEventId");
      fieldFn.accept(DwcTerm.institutionCode, "institutionCode");
      fieldFn.accept(DwcTerm.collectionCode, "collectionCode");
      fieldFn.accept(DwcTerm.catalogNumber, "catalogNumber");
      fieldFn.accept(DwcTerm.occurrenceID, "occurrenceId");

      // Core
      ObjectNode coreNode = JsonConverter.createObjectNode();
      core.forEach((k, v) -> Optional.ofNullable(v).ifPresent(x -> jc.addJsonRawField(coreNode, k, x)));

      // Extensions
      ObjectNode extNode = JsonConverter.createObjectNode();
      ext.forEach((k, v) -> {
        if (v != null && !v.isEmpty()) {
          ArrayNode extArrayNode = JsonConverter.createArrayNode();
          v.forEach(m -> {
            ObjectNode ns = JsonConverter.createObjectNode();
            m.forEach((ks, vs) -> Optional.ofNullable(vs).filter(v1 -> !v1.isEmpty())
                .ifPresent(x -> jc.addJsonRawField(ns, ks, x)));
            extArrayNode.add(ns);
          });
          extNode.set(k, extArrayNode);
        }
      });

      // Verbatim
      ObjectNode verbatimNode = JsonConverter.createObjectNode();
      verbatimNode.set("core", coreNode);
      verbatimNode.set("extensions", extNode);

      //Copy to all field
      Set<TextNode> allFieldValues = new HashSet<>();
      core.entrySet().stream()
          .filter(s -> !EXCLUDE_ALL.contains(s.getKey()))
          .forEach(s -> Optional.ofNullable(s.getValue()).ifPresent(v1 -> allFieldValues.add(getEscapedTextNode(v1))));
      ext.forEach((k, v) -> Optional.ofNullable(v).ifPresent(v1 ->
          v1.forEach(v2 -> {
            v2.forEach((k2, v3) -> Optional.ofNullable(v3).ifPresent(v4 -> allFieldValues.add(getEscapedTextNode(v4))));
          })));
      jc.getMainNode().putArray("all").addAll(allFieldValues);

      // Main node
      jc.getMainNode().set("verbatim", verbatimNode);

      //Classification verbatim
      ObjectNode classificationNode = jc.getMainNode().has("gbifClassification")
          ? (ObjectNode) jc.getMainNode().get("gbifClassification") : JsonConverter.createObjectNode();
      Optional.ofNullable(coreNode.get(DwcTerm.taxonID.qualifiedName()))
          .ifPresent(taxonID -> classificationNode.set(DwcTerm.taxonID.simpleName(), taxonID));
      Optional.ofNullable(coreNode.get(DwcTerm.scientificName.qualifiedName()))
          .ifPresent(verbatimScientificName -> classificationNode.set(GbifTerm.verbatimScientificName.simpleName(),
              verbatimScientificName));
      if (!jc.getMainNode().has("gbifClassification") && classificationNode.size() > 0) {
        jc.addJsonObject("gbifClassification", classificationNode);
      }

    };
  }

  /**
   * String converter for {@link LocationRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "location": {"lon": 10, "lat": 10},
   * "continent": "NORTH_AMERICA",
   * "waterBody": null,
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getLocationRecordConverter() {
    return (jc, record) -> {
      LocationRecord lr = (LocationRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, lr.getId());
      }

      if (lr.getDecimalLongitude() != null && lr.getDecimalLatitude() != null) {
        ObjectNode node = JsonConverter.createObjectNode();
        node.put("lon", lr.getDecimalLongitude());
        node.put("lat", lr.getDecimalLatitude());
        //geo_point
        jc.addJsonObject("coordinates", node);

        jc.getMainNode().put("decimalLatitude", lr.getDecimalLatitude());
        jc.getMainNode().put("decimalLongitude", lr.getDecimalLongitude());
        //geo_shape
        jc.addJsonTextFieldNoCheck("scoordinates",
            "POINT (" + lr.getDecimalLongitude() + " " + lr.getDecimalLatitude() + ")");

      }
      // Fields as a common view - "key": "value"
      jc.addCommonFields(record);
    };
  }

  /**
   * String converter for {@link TemporalRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "startDate": "10/10/2010",
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getTemporalRecordConverter() {
    return (jc, record) -> {
      TemporalRecord tr = (TemporalRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, tr.getId());
      }

      if (tr.getEventDate() != null && tr.getEventDate().getGte() != null) {
        jc.addJsonTextFieldNoCheck("eventDateSingle", tr.getEventDate().getGte());
      } else {
        TemporalUtils.getTemporal(tr.getYear(), tr.getMonth(), tr.getDay())
            .ifPresent(x -> jc.addJsonTextFieldNoCheck("eventDateSingle", x.toString()));
      }

      // Fields as a common view - "key": "value"
      jc.addCommonFields(record);
    };
  }

  /**
   * String converter for {@link TaxonRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "gbifKingdom": "Animalia",
   *  //.....more fields
   * "usage": {
   *  "key": 2442896,
   *  "name": "Actinemys marmorata (Baird & Girard, 1852)",
   *  "rank": "SPECIES"
   * },
   * "classification": [
   *  {
   *    "key": 1,
   *    "name": "Animalia",
   *    "rank": "KINGDOM"
   *  },
   *  //.....more objects
   * ],
   * "acceptedUsage": null,
   * //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getTaxonomyRecordConverter() {
    return (jc, record) -> {
      TaxonRecord trOrg = (TaxonRecord) record;
      //Copy only the fields that are needed in the Index
      TaxonRecord.Builder trBuilder = TaxonRecord.newBuilder()
          .setAcceptedUsage(trOrg.getAcceptedUsage())
          .setClassification(trOrg.getClassification())
          .setSynonym(trOrg.getSynonym())
          .setUsage(trOrg.getUsage())
          .setUsageParsedName(trOrg.getUsageParsedName())
          .setDiagnostics(trOrg.getDiagnostics())
          .setIssues(null); //Issues are accumulated

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, trOrg.getId());
      }

      TaxonRecord tr = trBuilder.build();

      //Create a ObjectNode with the specific fields copied from the original record
      ObjectNode classificationNode =
          jc.getMainNode().has("gbifClassification") ? (ObjectNode) jc.getMainNode().get("gbifClassification") :
              JsonConverter.createObjectNode();
      jc.addCommonFields(tr, classificationNode);
      List<RankedName> classifications = tr.getClassification();
      Set<IntNode> taxonKey = new HashSet<>();

      Optional.ofNullable(tr.getUsage()).ifPresent(s -> taxonKey.add(IntNode.valueOf(s.getKey())));
      Optional.ofNullable(tr.getAcceptedUsage()).ifPresent(au -> taxonKey.add(IntNode.valueOf(au.getKey())));

      if (classifications != null && !classifications.isEmpty()) {
        //Creates a set of fields" kingdomKey, phylumKey, classKey, etc for convenient aggregation/facets
        StringJoiner pathJoiner = new StringJoiner("_");
        classifications.forEach(rankedName -> {
              String lwRank = rankedName.getRank().name().toLowerCase();
              classificationNode.put(lwRank + "Key", rankedName.getKey());
              classificationNode.put(lwRank, rankedName.getName());
              taxonKey.add(IntNode.valueOf(rankedName.getKey()));
              if (Objects.nonNull(tr.getUsage()) && tr.getUsage().getRank() != rankedName.getRank()) {
                pathJoiner.add(rankedName.getKey().toString());
              }
            }
        );
        classificationNode.put("classificationPath", "_" + pathJoiner.toString());
        //All key are concatenated to support a single taxonKey field
        ArrayNode taxonKeyNode = classificationNode.putArray("taxonKey");
        taxonKeyNode.addAll(taxonKey);
      }

      Optional.ofNullable(tr.getUsageParsedName()).ifPresent(pn -> { //Required by API V1
        ObjectNode usageParsedNameNode = (ObjectNode) classificationNode.get("usageParsedName");
        usageParsedNameNode.put("genericName", pn.getGenus() != null ? pn.getGenus() : pn.getUninomial());
      });

      if (!jc.getMainNode().has("gbifClassification")) {
        jc.addJsonObject("gbifClassification", classificationNode);
      }
    };
  }

  /**
   * String converter for {@link LocationFeatureRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "locationFeatureLayers": [
   *     {
   *      "key": "c22",
   *      "value": "234"
   *     },
   *     {
   *      "key": "c223",
   *      "value": "34"
   *     },
   *   ]
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getLocationFeatureRecordConverter() {
    return (jc, record) -> {
      LocationFeatureRecord asr = (LocationFeatureRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, asr.getId());
      }

      Optional.ofNullable(asr.getItems())
          .filter(i -> !i.isEmpty())
          .ifPresent(m -> {
            List<ObjectNode> nodes = new ArrayList<>(m.size());
            m.forEach((k, v) -> {
              ObjectNode node = JsonConverter.createObjectNode();
              node.put("key", k);
              node.put("value", v);
              nodes.add(node);
            });
            jc.addJsonArray("locationFeatureLayers", nodes);
          });
    };
  }

  /**
   * String converter for {@link AmplificationRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * {
   *  "id": "777",
   *  "amplificationItems": [
   *    {
   *      "name": "n",
   *      "identity": 3,
   *      "appliedScientificName": "sn",
   *      "matchType": "mt",
   *      "bitScore": 1,
   *      "expectValue": 2,
   *      "querySequence": "qs",
   *      "subjectSequence": "ss",
   *      "qstart": 5,
   *      "qend": 4,
   *      "sstart": 8,
   *      "send": 6,
   *      "distanceToBestMatch": "dm",
   *      "sequenceLength": 7
   *    }
   *  ]
   * }
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getAmplificationRecordConverter() {
    return (jc, record) -> {
      AmplificationRecord ar = (AmplificationRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, ar.getId());
      }

      List<ObjectNode> nodes = ar.getAmplificationItems().stream()
          .filter(x -> x.getBlastResult() != null && x.getBlastResult().getMatchType() != null)
          .map(x -> {
            BlastResult blast = x.getBlastResult();
            ObjectNode node = JsonConverter.createObjectNode();
            jc.addCommonFields(blast, node);
            return node;
          })
          .collect(Collectors.toList());

      jc.addJsonArray("amplificationItems", nodes);
    };
  }

  /**
   * String converter for {@link MeasurementOrFactRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * {
   *  "id": "777",
   *  "measurementOrFactItems": [
   *     {
   *       "id": "123",
   *       "type": "{\"something\":1}{\"something\":1}",
   *       "value": 1.1,
   *       "determinedDate": {
   *         "gte": "2010",
   *         "lte": "2011"
   *       }
   *     },
   *     {
   *       "id": "124",
   *       "type": null,
   *       "value": null,
   *       "determinedDate": {
   *         "gte": "2010",
   *         "lte": "2012"
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getMeasurementOrFactRecordConverter() {
    return (jc, record) -> {
      MeasurementOrFactRecord mfr = (MeasurementOrFactRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, mfr.getId());
      }

      List<ObjectNode> nodes = mfr.getMeasurementOrFactItems().stream()
          .filter(x -> x.getValueParsed() != null || x.getDeterminedDateParsed() != null)
          .map(x -> {
            ObjectNode node = JsonConverter.createObjectNode();
            node.put("id", x.getId());
            node.put("type", x.getType());
            node.put("value", x.getValueParsed());
            node.set("determinedDate", new POJONode(x.getDeterminedDateParsed()));
            return node;
          })
          .collect(Collectors.toList());

      jc.addJsonArray("measurementOrFactItems", nodes);
    };
  }

  /**
   * String converter for {@link MultimediaRecord}, convert an object to specific string view * *
   *
   * <pre>{@code
   * Result example:
   *
   * {
   *  "id": "777",
   *  "multimediaItems": [
   *    {
   *      "type": "StillImage",
   *      "format": "image/jpeg",
   *      "identifier": "http://arctos.database.museum/media/10436011?open"
   *     },
   *     {
   *      "type": "MovingImage",
   *      "format": "video/mp4",
   *      "identifier": "http://arctos.database.museum/media/10436025?open"
   *      }
   *  ],
   *  "mediaTypes": [
   *     "StillImage",
   *     "MovingImage"
   *  ]
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getMultimediaConverter() {
    return (jc, record) -> {
      MultimediaRecord mr = (MultimediaRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, mr.getId());
      }

      // multimedia items
      if (mr.getMultimediaItems() != null && !mr.getMultimediaItems().isEmpty()) {
        List<ObjectNode> items = mr.getMultimediaItems().stream()
            .map(item -> {
              ObjectNode node = JsonConverter.createObjectNode();

              BiConsumer<String, String> addField = (k, v) ->
                  Optional.ofNullable(v).filter(f -> !f.isEmpty()).ifPresent(r -> node.put(k, r));

              addField.accept("type", item.getType());
              addField.accept("format", item.getFormat());
              addField.accept("identifier", item.getIdentifier());
              addField.accept("audience", item.getAudience());
              addField.accept("contributor", item.getContributor());
              addField.accept("created", item.getCreated());
              addField.accept("creator", item.getCreator());
              addField.accept("description", item.getDescription());
              addField.accept("license", item.getLicense());
              addField.accept("publisher", item.getPublisher());
              addField.accept("references", item.getReferences());
              addField.accept("rightsHolder", item.getRightsHolder());
              addField.accept("source", item.getSource());
              addField.accept("title", item.getTitle());

              return node;
            })
            .collect(Collectors.toList());

        jc.addJsonArray("multimediaItems", items);

        // media types
        Set<TextNode> mediaTypes = mr.getMultimediaItems().stream()
            .filter(i -> !Strings.isNullOrEmpty(i.getType()))
            .map(Multimedia::getType)
            .map(TextNode::valueOf)
            .collect(Collectors.toSet());

        jc.addJsonArray("mediaTypes", mediaTypes);

        // media licenses
        Set<TextNode> mediaLicenses = mr.getMultimediaItems().stream()
            .filter(i -> !Strings.isNullOrEmpty(i.getLicense()))
            .map(Multimedia::getLicense)
            .map(TextNode::valueOf)
            .collect(Collectors.toSet());

        jc.addJsonArray("mediaLicenses", mediaLicenses);
      }
    };
  }


  /**
   * String converter for {@link TaggedValueRecord}, convert an object to specific string view.
   * Copies all the value at the root node level.
   *
   * <pre>{@code
   * Result example:
   *
   * "verbatim": {
   *    ...
   * },
   * "institutionKey": "7ddf754f-d193-4cc9-b351-99906754a03b",
   * "collectionKey": "7ddf754f-d193-4cc9-b351-99906754a07b",
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getTaggedValueConverter() {
    return (jc, record) -> {
      TaggedValueRecord tvr = (TaggedValueRecord) record;
      if (Objects.nonNull(tvr.getTaggedValues())) {
        tvr.getTaggedValues().forEach((k, v) ->
            Optional.ofNullable(TERM_FACTORY.findTerm(k))
                .ifPresent(term -> jc.addJsonTextFieldNoCheck(term.simpleName(), v))
        );
      }

    };
  }

  /**
   * String converter for {@link BasicRecord}, convert an object to specific string view.
   * Copies all the value at the root node level.
   * <p>
   * gbif/portal-feedback#2423
   * Preserve record-level licences over dataset-level ones
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getBasicRecordConverter() {
    return (jc, record) -> {
      BasicRecord br = (BasicRecord) record;

      // Use BasicRecord license insted of json node
      JsonNode node = jc.getMainNode().get("license");
      if (node != null) {
        String license = node.asText();
        if (br.getLicense() == null || br.getLicense().equals(License.UNSPECIFIED.name())
            || br.getLicense().equals(License.UNSUPPORTED.name())) {
          br.setLicense(license);
        }
      }

      // Add other fields
      jc.addCommonFields(br);
    };
  }
}
