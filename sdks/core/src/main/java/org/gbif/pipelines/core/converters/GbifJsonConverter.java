package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

  private final JsonConverter.JsonConverterBuilder builder =
      JsonConverter.builder()
          .skipKey("decimalLatitude")
          .skipKey("decimalLongitude")
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(LocationRecord.class, getLocationRecordConverter())
          .converter(TemporalRecord.class, getTemporalRecordConverter())
          .converter(TaxonRecord.class, getTaxonomyRecordConverter())
          .converter(AustraliaSpatialRecord.class, getAustraliaSpatialRecordConverter());

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
      builder.skipKey("id");
    }
    if (skipIssues) {
      builder.skipKey("issues");
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
    mainNode.set("issues", issueArrayNodes);

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
   *   "continent": "North America",
   *   //.....more fields
   * },
   * "basisOfRecord": null,
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<JsonConverter, SpecificRecordBase> getExtendedRecordConverter() {
    return (jc, record) -> {

      ExtendedRecord er = (ExtendedRecord) record;

      jc.addJsonTextFieldNoCheck("id", er.getId());

      Map<String, String> core = er.getCoreTerms();
      Map<String, List<Map<String, String>>> ext = er.getExtensions();

      Optional.ofNullable(core.get(DwcTerm.recordedBy.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("recordedBy", x));
      Optional.ofNullable(core.get(DwcTerm.organismID.qualifiedName()))
          .ifPresent(x -> jc.addJsonTextFieldNoCheck("organismId", x));

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
        jc.addJsonTextFieldNoCheck("id", lr.getId());
      }

      if (lr.getDecimalLongitude() != null && lr.getDecimalLatitude() != null) {
        ObjectNode node = JsonConverter.createObjectNode();
        node.put("lon", lr.getDecimalLongitude().toString());
        node.put("lat", lr.getDecimalLatitude().toString());
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
        jc.addJsonTextFieldNoCheck("id", tr.getId());
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
      TaxonRecord tr = (TaxonRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck("id", tr.getId());
      }

      List<RankedName> classifications = tr.getClassification();
      if (classifications != null && !classifications.isEmpty()) {
        List<ObjectNode> nodes = new ArrayList<>(classifications.size());
        for (int i = 0; i < classifications.size(); i++) {
          RankedName name = classifications.get(i);
          ObjectNode node = JsonConverter.createObjectNode();
          node.put("taxonKey", name.getKey());
          node.put("name", name.getName());
          node.put("depthKey_" + i, name.getKey());
          node.put("kingdomKey", name.getKey());
          node.put("rank", name.getRank().name());
          nodes.add(node);
        }
        jc.addJsonArray("backbone", nodes);
      }

      // Other Gbif fields
      Optional.ofNullable(tr.getUsage()).ifPresent(
          usage -> {
            jc.addJsonTextFieldNoCheck("gbifTaxonKey", usage.getKey().toString());
            jc.addJsonTextFieldNoCheck("gbifScientificName", usage.getName());
            jc.addJsonTextFieldNoCheck("gbifTaxonRank", usage.getRank().name());
          });
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
        jc.addJsonTextFieldNoCheck("id", asr.getId());
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
}
