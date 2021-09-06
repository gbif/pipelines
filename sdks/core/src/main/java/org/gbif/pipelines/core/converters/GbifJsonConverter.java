package org.gbif.pipelines.core.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.BlastResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

/**
 * Converter for objects to GBIF elasticsearch schema. You can pass any {@link SpecificRecordBase}
 * objects(Avro generated)
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

  private static final Set<String> EXCLUDE_ALL =
      Collections.singleton(DwcTerm.footprintWKT.qualifiedName());

  private static final Set<String> INCLUDE_EXT_ALL =
      new HashSet<>(
          Arrays.asList(
              Extension.MULTIMEDIA.getRowType(),
              Extension.AUDUBON.getRowType(),
              Extension.IMAGE.getRowType()));

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final LongFunction<LocalDateTime> DATE_FN =
      l -> LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));

  private final JsonConverter.JsonConverterBuilder builder =
      JsonConverter.builder()
          .skipKey(Indexing.DECIMAL_LATITUDE)
          .skipKey(Indexing.DECIMAL_LONGITUDE)
          .skipKey(Indexing.MACHINE_TAGS)
          .skipKey(Indexing.CREATED)
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(LocationRecord.class, getLocationRecordConverter())
          .converter(TemporalRecord.class, getTemporalRecordConverter())
          .converter(TaxonRecord.class, getTaxonomyRecordConverter())
          .converter(LocationFeatureRecord.class, getLocationFeatureRecordConverter())
          .converter(AmplificationRecord.class, getAmplificationRecordConverter())
          .converter(MultimediaRecord.class, getMultimediaConverter())
          .converter(BasicRecord.class, getBasicRecordConverter())
          .converter(GrscicollRecord.class, getGrscicollRecordConverter());

  @Builder.Default private boolean skipIssues = false;

  @Builder.Default private boolean skipId = true;

  @Singular private List<SpecificRecordBase> records;

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * the new ES record
   */
  public static ObjectNode toJson(SpecificRecordBase... records) {
    return GbifJsonConverter.builder().records(Arrays.asList(records)).build().toJson();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * a partial ES record update
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
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object,
   * suited to the new ES record
   */
  public static String toStringJson(SpecificRecordBase... records) {
    return toJson(records).toString();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object,
   * suited to a partial ES record update
   */
  public static String toStringPartialJson(SpecificRecordBase... records) {
    return toPartialJson(records).toString();
  }

  /** Change the json result, merging all issues from records to one array */
  public ObjectNode toJson() {
    builder.records(records);
    if (skipId) {
      builder.skipKey(Indexing.ID);
    }
    if (skipIssues) {
      builder.skipKey(Indexing.ISSUES);
    }

    ObjectNode mainNode = builder.build().toJson();

    if (!skipIssues) {
      addIssues(mainNode);
    }

    getMaxCreationDate(mainNode)
        .ifPresent(
            createdDate -> mainNode.set(Indexing.CREATED, new TextNode(createdDate.toString())));

    Optional.ofNullable(mainNode.get("lastCrawled"))
        .ifPresent(
            lastCrawled ->
                mainNode.set(
                    "lastCrawled", new TextNode(DATE_FN.apply(lastCrawled.asLong()).toString())));

    return mainNode;
  }

  /** Gets the maximum/latest created date of all the records. */
  private Optional<LocalDateTime> getMaxCreationDate(ObjectNode rootNode) {
    return Optional.ofNullable(rootNode.get(Indexing.CREATED))
        .map(created -> Optional.of(DATE_FN.apply(rootNode.get(Indexing.CREATED).asLong())))
        .orElseGet(
            () ->
                records.stream()
                    .filter(
                        record -> Objects.nonNull(record.getSchema().getField(Indexing.CREATED)))
                    .map(record -> record.get(Indexing.CREATED))
                    .filter(Objects::nonNull)
                    .map(x -> DATE_FN.apply((Long) x))
                    .max(LocalDateTime::compareTo));
  }

  @Override
  public String toString() {
    return toJson().toString();
  }

  /** Adds issues and notIssues json nodes */
  private void addIssues(ObjectNode mainNode) {
    // Issues
    Set<String> issues =
        records.stream()
            .filter(Issues.class::isInstance)
            .flatMap(x -> ((Issues) x).getIssues().getIssueList().stream())
            .collect(Collectors.toSet());
    ArrayNode issueArrayNodes = JsonConverter.createArrayNode();
    issues.forEach(issueArrayNodes::add);
    mainNode.set(Indexing.ISSUES, issueArrayNodes);

    // Not issues
    Set<String> notIssues =
        Arrays.stream(OccurrenceIssue.values())
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

      jc.addJsonTextFieldNoCheck(Indexing.ID, er.getId());

      Map<String, String> core = er.getCoreTerms();

      BiConsumer<Term, String> fieldFn =
          (t, k) ->
              Optional.ofNullable(core.get(t.qualifiedName()))
                  .ifPresent(r -> jc.addJsonTextFieldNoCheck(k, r));

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
      core.forEach(
          (k, v) -> Optional.ofNullable(v).ifPresent(x -> jc.addJsonRawField(coreNode, k, x)));

      // Extensions
      ObjectNode extNode = JsonConverter.createObjectNode();
      Map<String, List<Map<String, String>>> ext = er.getExtensions();
      ext.forEach(
          (k, v) -> {
            if (v != null && !v.isEmpty()) {
              ArrayNode extArrayNode = JsonConverter.createArrayNode();
              v.forEach(
                  m -> {
                    ObjectNode ns = JsonConverter.createObjectNode();
                    m.forEach(
                        (ks, vs) ->
                            Optional.ofNullable(vs)
                                .filter(v1 -> !v1.isEmpty())
                                .ifPresent(x -> jc.addJsonRawField(ns, ks, x)));
                    extArrayNode.add(ns);
                  });
              extNode.set(k, extArrayNode);
            }
          });

      // Has extensions
      ArrayNode extNameNode = JsonConverter.createArrayNode();
      ext.entrySet().stream()
          .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
          .forEach(x -> extNameNode.add(x.getKey()));
      jc.getMainNode().set("extensions", extNameNode);

      // Verbatim
      ObjectNode verbatimNode = JsonConverter.createObjectNode();
      verbatimNode.set("core", coreNode);
      verbatimNode.set("extensions", extNode);

      // Copy to all field
      Set<String> allFieldValues = new HashSet<>();

      core.entrySet().stream()
          .filter(termValue -> !EXCLUDE_ALL.contains(termValue.getKey()))
          .filter(termValue -> termValue.getValue() != null)
          .map(Entry::getValue)
          .forEach(allFieldValues::add);

      ext.entrySet().stream()
          .filter(kv -> INCLUDE_EXT_ALL.contains(kv.getKey()))
          .map(Entry::getValue)
          .filter(Objects::nonNull)
          .forEach(
              extension ->
                  extension.forEach(
                      termValueMap ->
                          termValueMap.values().stream()
                              .filter(Objects::nonNull)
                              .forEach(allFieldValues::add)));

      Set<TextNode> textNodeMap =
          allFieldValues.stream()
              .map(JsonConverter::getEscapedTextNode)
              .collect(Collectors.toSet());

      jc.getMainNode().putArray("all").addAll(textNodeMap);

      // Main node
      jc.getMainNode().set("verbatim", verbatimNode);

      // Classification verbatim
      ObjectNode classificationNode =
          jc.getMainNode().has("gbifClassification")
              ? (ObjectNode) jc.getMainNode().get("gbifClassification")
              : JsonConverter.createObjectNode();
      Optional.ofNullable(coreNode.get(DwcTerm.taxonID.qualifiedName()))
          .ifPresent(taxonID -> classificationNode.set(DwcTerm.taxonID.simpleName(), taxonID));
      Optional.ofNullable(coreNode.get(DwcTerm.scientificName.qualifiedName()))
          .ifPresent(
              verbatimScientificName ->
                  classificationNode.set(
                      GbifTerm.verbatimScientificName.simpleName(), verbatimScientificName));
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
        jc.addJsonTextFieldNoCheck(Indexing.ID, lr.getId());
      }

      if (lr.getDecimalLongitude() != null && lr.getDecimalLatitude() != null) {
        ObjectNode node = JsonConverter.createObjectNode();
        node.put("lon", lr.getDecimalLongitude());
        node.put("lat", lr.getDecimalLatitude());
        // geo_point
        jc.addJsonObject("coordinates", node);

        jc.getMainNode().put("decimalLatitude", lr.getDecimalLatitude());
        jc.getMainNode().put("decimalLongitude", lr.getDecimalLongitude());
        // geo_shape
        jc.addJsonTextFieldNoCheck(
            "scoordinates",
            "POINT (" + lr.getDecimalLongitude() + " " + lr.getDecimalLatitude() + ")");
      }

      // Fields as a common view - "key": "value"
      jc.addCommonFields(record);

      // All GADM GIDs as an array, for searching at multiple levels.
      if (lr.getGadm() != null) {
        GadmFeatures gadmFeatures = lr.getGadm();
        ArrayNode arrayGadmGidNode = JsonConverter.createArrayNode();
        Optional.ofNullable(gadmFeatures.getLevel0Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel1Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel2Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel3Gid()).ifPresent(arrayGadmGidNode::add);
        ObjectNode gadmNode =
            jc.getMainNode().has("gadm")
                ? (ObjectNode) jc.getMainNode().get("gadm")
                : JsonConverter.createObjectNode();
        gadmNode.set("gids", arrayGadmGidNode);
      }
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
        jc.addJsonTextFieldNoCheck(Indexing.ID, tr.getId());
      }

      Optional<TemporalAccessor> tao;
      if (tr.getEventDate() != null && tr.getEventDate().getGte() != null) {
        tao =
            Optional.ofNullable(tr.getEventDate().getGte())
                .map(StringToDateFunctions.getStringToTemporalAccessor());
      } else {
        tao = TemporalConverter.from(tr.getYear(), tr.getMonth(), tr.getDay());
      }
      tao.map(ta -> TemporalAccessorUtils.toEarliestLocalDateTime(ta, true))
          .ifPresent(d -> jc.addJsonTextFieldNoCheck("eventDateSingle", d.toString()));

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
      // Copy only the fields that are needed in the Index
      TaxonRecord.Builder trBuilder =
          TaxonRecord.newBuilder()
              .setAcceptedUsage(trOrg.getAcceptedUsage())
              .setClassification(trOrg.getClassification())
              .setSynonym(trOrg.getSynonym())
              .setUsage(trOrg.getUsage())
              .setUsageParsedName(trOrg.getUsageParsedName())
              .setDiagnostics(trOrg.getDiagnostics())
              .setIucnRedListCategoryCode(trOrg.getIucnRedListCategoryCode())
              .setIssues(null); // Issues are accumulated

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(Indexing.ID, trOrg.getId());
      }

      TaxonRecord tr = trBuilder.build();

      // Create a ObjectNode with the specific fields copied from the original record
      ObjectNode classificationNode =
          jc.getMainNode().has("gbifClassification")
              ? (ObjectNode) jc.getMainNode().get("gbifClassification")
              : JsonConverter.createObjectNode();
      jc.addCommonFields(tr, classificationNode);
      List<RankedName> classifications = tr.getClassification();
      Set<IntNode> taxonKey = new HashSet<>();

      Optional.ofNullable(tr.getUsage()).ifPresent(s -> taxonKey.add(IntNode.valueOf(s.getKey())));
      Optional.ofNullable(tr.getAcceptedUsage())
          .ifPresent(au -> taxonKey.add(IntNode.valueOf(au.getKey())));

      if (classifications != null && !classifications.isEmpty()) {
        // Creates a set of fields" kingdomKey, phylumKey, classKey, etc for convenient
        // aggregation/facets
        StringJoiner pathJoiner = new StringJoiner("_");
        classifications.forEach(
            rankedName -> {
              String lwRank = rankedName.getRank().name().toLowerCase();
              classificationNode.put(lwRank + "Key", rankedName.getKey());
              classificationNode.put(lwRank, rankedName.getName());
              taxonKey.add(IntNode.valueOf(rankedName.getKey()));
              if (Objects.nonNull(tr.getUsage())
                  && tr.getUsage().getRank() != rankedName.getRank()) {
                pathJoiner.add(rankedName.getKey().toString());
              }
            });
        classificationNode.put("classificationPath", "_" + pathJoiner.toString());
        // All key are concatenated to support a single taxonKey field
        ArrayNode taxonKeyNode = classificationNode.putArray("taxonKey");
        taxonKeyNode.addAll(taxonKey);
      }

      Optional.ofNullable(tr.getUsageParsedName())
          .ifPresent(
              pn -> { // Required by API V1
                ObjectNode usageParsedNameNode =
                    (ObjectNode) classificationNode.get("usageParsedName");
                usageParsedNameNode.put(
                    "genericName", pn.getGenus() != null ? pn.getGenus() : pn.getUninomial());
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
        jc.addJsonTextFieldNoCheck(Indexing.ID, asr.getId());
      }

      Optional.ofNullable(asr.getItems())
          .filter(i -> !i.isEmpty())
          .ifPresent(
              m -> {
                List<ObjectNode> nodes = new ArrayList<>(m.size());
                m.forEach(
                    (k, v) -> {
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
        jc.addJsonTextFieldNoCheck(Indexing.ID, ar.getId());
      }

      List<ObjectNode> nodes =
          ar.getAmplificationItems().stream()
              .filter(x -> x.getBlastResult() != null && x.getBlastResult().getMatchType() != null)
              .map(
                  x -> {
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
        jc.addJsonTextFieldNoCheck(Indexing.ID, mr.getId());
      }

      // multimedia items
      if (mr.getMultimediaItems() != null && !mr.getMultimediaItems().isEmpty()) {
        List<ObjectNode> items =
            mr.getMultimediaItems().stream()
                .map(
                    item -> {
                      ObjectNode node = JsonConverter.createObjectNode();

                      BiConsumer<String, String> addField =
                          (k, v) ->
                              Optional.ofNullable(v)
                                  .filter(f -> !f.isEmpty())
                                  .ifPresent(r -> node.put(k, r));

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
        Set<TextNode> mediaTypes =
            mr.getMultimediaItems().stream()
                .filter(i -> !Strings.isNullOrEmpty(i.getType()))
                .map(Multimedia::getType)
                .map(TextNode::valueOf)
                .collect(Collectors.toSet());

        jc.addJsonArray("mediaTypes", mediaTypes);

        // media licenses
        Set<TextNode> mediaLicenses =
            mr.getMultimediaItems().stream()
                .filter(i -> !Strings.isNullOrEmpty(i.getLicense()))
                .map(Multimedia::getLicense)
                .map(TextNode::valueOf)
                .collect(Collectors.toSet());

        jc.addJsonArray("mediaLicenses", mediaLicenses);
      }
    };
  }

  /**
   * String converter for {@link BasicRecord}, convert an object to specific string view. Copies all
   * the value at the root node level.
   *
   * <p>gbif/portal-feedback#2423 Preserve record-level licences over dataset-level ones
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getBasicRecordConverter() {
    return (jc, record) -> {
      BasicRecord br = (BasicRecord) record;

      // Use BasicRecord license insted of json node
      JsonNode node = jc.getMainNode().get("license");
      if (node != null) {
        String license = node.asText();
        if (br.getLicense() == null
            || br.getLicense().equals(License.UNSPECIFIED.name())
            || br.getLicense().equals(License.UNSUPPORTED.name())) {
          br.setLicense(license);
        }
      }

      // Add other fields
      jc.addCommonFields(br);
    };
  }

  /**
   * String converter for {@link GrscicollRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "institutionKey": "04ec1770-6216-4c66-b9ea-c8087b8f563f",
   * "collectionKey": "02d1e772-54ee-4767-b4b8-c35f0c7270ba",
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getGrscicollRecordConverter() {
    return (jc, record) -> {
      GrscicollRecord gr = (GrscicollRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(Indexing.ID, gr.getId());
      }

      if (gr.getInstitutionMatch() != null) {
        String institutionKey = gr.getInstitutionMatch().getKey();
        if (institutionKey != null) {
          jc.addJsonTextFieldNoCheck(GbifInternalTerm.institutionKey.simpleName(), institutionKey);
        }
      }

      if (gr.getCollectionMatch() != null) {
        String collectionKey = gr.getCollectionMatch().getKey();
        if (collectionKey != null) {
          jc.addJsonTextFieldNoCheck(GbifInternalTerm.collectionKey.simpleName(), collectionKey);
        }
      }
    };
  }
}
