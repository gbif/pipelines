package org.gbif.pipelines.core.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Converter for objects to GBIF elasticsearch schema. You can pass any {@link SpecificRecordBase}
 * objects(Avro generated)
 *
 * <pre>{@code
 * Usage example:
 *
 * ExtendedRecord er = ...
 * IdentifierRecord ir =  ...
 * EventCoreRecord ecr =  ...
 * String result = EventCoreJsonConverter.toStringJson(er, ir, ecr);
 *
 * }</pre>
 */
@SuppressWarnings("FallThrough")
@Slf4j
@Builder
@Deprecated
public class DeprecatedEventJsonConverter {

  private static final Set<String> EXCLUDE_ALL = Collections.emptySet();
  private static final Set<String> INCLUDE_EXT_ALL = Collections.emptySet();

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final LongFunction<LocalDateTime> DATE_FN =
      l -> LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));

  private final GenericJsonConverter.GenericJsonConverterBuilder builder =
      GenericJsonConverter.builder()
          .skipKey(Indexing.DECIMAL_LATITUDE)
          .skipKey(Indexing.DECIMAL_LONGITUDE)
          .skipKey(Indexing.MACHINE_TAGS)
          .skipKey(Indexing.CREATED)
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(IdentifierRecord.class, geIdentifierRecordConverter())
          .converter(LocationRecord.class, getLocationRecordConverter())
          .converter(TemporalRecord.class, getTemporalRecordConverter())
          .converter(MultimediaRecord.class, getMultimediaConverter())
          .converter(EventCoreRecord.class, geEventCoreRecordConverter());

  @Builder.Default private boolean skipIssues = false;

  @Builder.Default private boolean skipId = true;

  @Singular private List<SpecificRecordBase> records;

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * the new ES record
   */
  public static ObjectNode toJson(SpecificRecordBase... records) {
    return DeprecatedEventJsonConverter.builder().records(Arrays.asList(records)).build().toJson();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * a partial ES record update
   */
  public static ObjectNode toPartialJson(SpecificRecordBase... records) {
    return DeprecatedEventJsonConverter.builder()
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
                    .filter(r -> Objects.nonNull(r.getSchema().getField(Indexing.CREATED)))
                    .map(r -> r.get(Indexing.CREATED))
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
    ArrayNode issueArrayNodes = GenericJsonConverter.createArrayNode();
    issues.forEach(issueArrayNodes::add);
    mainNode.set(Indexing.ISSUES, issueArrayNodes);

    // Not issues
    Set<String> notIssues =
        Arrays.stream(OccurrenceIssue.values())
            .map(Enum::name)
            .filter(x -> !issues.contains(x))
            .collect(Collectors.toSet());

    ArrayNode arrayNotIssuesNode = GenericJsonConverter.createArrayNode();
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
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> getExtendedRecordConverter() {
    return (jc, record) -> {
      ExtendedRecord er = (ExtendedRecord) record;

      jc.addJsonTextFieldNoCheck(Indexing.ID, er.getId());

      Map<String, String> core = er.getCoreTerms();

      BiConsumer<Term, String> fieldFn =
          (t, k) ->
              Optional.ofNullable(core.get(t.qualifiedName()))
                  .ifPresent(r -> jc.addJsonTextFieldNoCheck(k, r));

      fieldFn.accept(DwcTerm.eventID, "eventId");
      fieldFn.accept(DwcTerm.parentEventID, "parentEventId");
      fieldFn.accept(DwcTerm.institutionCode, "institutionCode");

      // Core
      ObjectNode coreNode = GenericJsonConverter.createObjectNode();
      core.forEach(
          (k, v) -> Optional.ofNullable(v).ifPresent(x -> jc.addJsonRawField(coreNode, k, x)));

      // Extensions
      ObjectNode extNode = GenericJsonConverter.createObjectNode();
      Map<String, List<Map<String, String>>> ext = er.getExtensions();
      ext.forEach(
          (k, v) -> {
            if (v != null && !v.isEmpty()) {
              ArrayNode extArrayNode = GenericJsonConverter.createArrayNode();
              v.forEach(
                  m -> {
                    ObjectNode ns = GenericJsonConverter.createObjectNode();
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
      ArrayNode extNameNode = GenericJsonConverter.createArrayNode();
      ext.entrySet().stream()
          .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
          .forEach(x -> extNameNode.add(x.getKey()));
      jc.getMainNode().set("extensions", extNameNode);

      // Verbatim
      ObjectNode verbatimNode = GenericJsonConverter.createObjectNode();
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
              .flatMap(v -> Stream.of(v.split(ModelUtils.DEFAULT_SEPARATOR)))
              .map(GenericJsonConverter::getEscapedTextNode)
              .collect(Collectors.toSet());

      jc.getMainNode().putArray("all").addAll(textNodeMap);

      // Main node
      jc.getMainNode().set("verbatim", verbatimNode);
    };
  }

  /**
   * String converter for {@link EventCoreRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> geIdentifierRecordConverter() {
    return (jc, record) -> {
      IdentifierRecord ir = (IdentifierRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(Indexing.ID, ir.getId());
      }

      // Fields as a common view - "key": "value"
      jc.addCommonFields(record);
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
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> getLocationRecordConverter() {
    return (jc, record) -> {
      LocationRecord lr = (LocationRecord) record;

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(Indexing.ID, lr.getId());
      }

      if (lr.getDecimalLongitude() != null && lr.getDecimalLatitude() != null) {
        ObjectNode node = GenericJsonConverter.createObjectNode();
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
        ArrayNode arrayGadmGidNode = GenericJsonConverter.createArrayNode();
        Optional.ofNullable(gadmFeatures.getLevel0Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel1Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel2Gid()).ifPresent(arrayGadmGidNode::add);
        Optional.ofNullable(gadmFeatures.getLevel3Gid()).ifPresent(arrayGadmGidNode::add);
        ObjectNode gadmNode =
            jc.getMainNode().has("gadm")
                ? (ObjectNode) jc.getMainNode().get("gadm")
                : GenericJsonConverter.createObjectNode();
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
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> getTemporalRecordConverter() {
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
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> getMultimediaConverter() {
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
                      ObjectNode node = GenericJsonConverter.createObjectNode();

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
   * String converter for {@link EventCoreRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<GenericJsonConverter, SpecificRecordBase> geEventCoreRecordConverter() {
    return (jc, record) -> {
      EventCoreRecord ecr = (EventCoreRecord) record;

      // Use BasicRecord license insted of json node
      JsonNode node = jc.getMainNode().get("license");
      if (node != null) {
        String license = node.asText();
        if (ecr.getLicense() == null
            || ecr.getLicense().equals(License.UNSPECIFIED.name())
            || ecr.getLicense().equals(License.UNSUPPORTED.name())) {
          ecr.setLicense(license);
        }
      }

      BiConsumer<List<String>, String> joinValuesSetter =
          (values, fieldName) -> {
            if (values != null && !values.isEmpty()) {
              String joinedValues = String.join("|", values);
              jc.addJsonTextFieldNoCheck(fieldName, joinedValues);
            }
          };

      joinValuesSetter.accept(ecr.getSamplingProtocol(), Indexing.SAMPLING_PROTOCOL_JOINED);

      // Add other fields
      jc.addCommonFields(ecr);
    };
  }
}
