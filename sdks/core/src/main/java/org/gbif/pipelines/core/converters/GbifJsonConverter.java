package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.BlastResult;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

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

  private final JsonConverter.JsonConverterBuilder builder =
      JsonConverter.builder()
          .skipKey("decimalLatitude")
          .skipKey("decimalLongitude")
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(LocationRecord.class, getLocationRecordConverter())
          .converter(TemporalRecord.class, getTemporalRecordConverter())
          .converter(TaxonRecord.class, getTaxonomyRecordConverter())
          .converter(AustraliaSpatialRecord.class, getAustraliaSpatialRecordConverter())
          .converter(AmplificationRecord.class, getAmplificationRecordConverter())
          .converter(MeasurementOrFactRecord.class, getMeasurementOrFactRecordConverter());

  @Builder.Default
  private boolean skipIssues = false;

  @Builder.Default
  private boolean skipId = true;

  @Singular
  private List<SpecificRecordBase> records;

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object, suited to the new ES
   * record
   */
  public static String toStringJson(SpecificRecordBase... records) {
    return GbifJsonConverter.builder()
        .records(Arrays.asList(records))
        .build()
        .toString();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into string json object, suited to a partial ES
   * record update
   */
  public static String toStringPartialJson(SpecificRecordBase... records) {
    return GbifJsonConverter.builder()
        .records(Arrays.asList(records))
        .skipId(false)
        .skipIssues(true)
        .build()
        .toString();
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

    return mainNode;
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

      Optional.ofNullable(core.get(DwcTerm.recordedBy.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("recordedBy", x));
      Optional.ofNullable(core.get(DwcTerm.recordNumber.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("recordNumber", x));
      Optional.ofNullable(core.get(DwcTerm.organismID.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("organismId", x));
      Optional.ofNullable(core.get(DwcTerm.samplingProtocol.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("samplingProtocol", x));
      Optional.ofNullable(core.get(DwcTerm.eventID.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("eventId", x));
      Optional.ofNullable(core.get(DwcTerm.parentEventID.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("parentEventId", x));
      Optional.ofNullable(core.get(DwcTerm.institutionCode.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("institutionCode", x));
      Optional.ofNullable(core.get(DwcTerm.collectionCode.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("collectionCode", x));
      Optional.ofNullable(core.get(DwcTerm.catalogNumber.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("catalogNumber", x));
      Optional.ofNullable(core.get(DwcTerm.occurrenceID.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("occurrenceId", x));

      // Core
      ObjectNode coreNode = JsonConverter.createObjectNode();
      core.forEach((k, v) -> jc.addJsonRawField(coreNode, k, v));

      // Extensions
      ObjectNode extNode = JsonConverter.createObjectNode();
      ext.forEach((k, v) -> {
        ArrayNode extArrayNode = JsonConverter.createArrayNode();
        v.forEach(m -> {
          ObjectNode ns = JsonConverter.createObjectNode();
          m.forEach((ks, vs) -> jc.addJsonRawField(ns, ks, vs));
          extArrayNode.add(ns);
        });
        extNode.set(k, extArrayNode);
      });

      // Verbatim
      ObjectNode verbatimNode = JsonConverter.createObjectNode();
      verbatimNode.set("core", coreNode);
      verbatimNode.set("extensions", extNode);

      // Main node
      jc.getMainNode().set("verbatim", verbatimNode);
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
        jc.addJsonObject("coordinates", node);
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

      Optional.ofNullable(tr.getEventDate())
          .map(EventDate::getGte)
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("startDate", x));

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
                        .setUsage(trOrg.getUsage());

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(ID, trOrg.getId());
      }

      TaxonRecord tr = trBuilder.build();


      //Create a ObjectNode with the specific fields copied from the original record
      ObjectNode classificationNode = JsonConverter.createObjectNode();
      jc.addCommonFields(tr, classificationNode);
      List<RankedName> classifications = tr.getClassification();
      if (classifications != null && !classifications.isEmpty()) {
        //Creates a set of fields" kingdomKey, phylumKey, classKey, etc for convenient aggregation/facets
        StringJoiner pathJoiner = new StringJoiner("_");
        classifications.forEach(rankedName -> {
            classificationNode.put(rankedName.getRank().name().toLowerCase() + "Key", rankedName.getKey());
            if (Objects.nonNull(tr.getUsage()) && tr.getUsage().getRank() != rankedName.getRank()) {
              pathJoiner.add(rankedName.getKey().toString());
            }
          }
        );
        classificationNode.put("classificationPath", "_"  + pathJoiner.toString());
      }
      jc.addJsonObject("gbifClassification", classificationNode);
    };
  }

  /**
   * String converter for {@link AustraliaSpatialRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "australiaSpatialLayers": [
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
  private BiConsumer<JsonConverter, SpecificRecordBase> getAustraliaSpatialRecordConverter() {
    return (jc, record) -> {
      AustraliaSpatialRecord asr = (AustraliaSpatialRecord) record;

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
            jc.addJsonArray("australiaSpatialLayers", nodes);
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
}
