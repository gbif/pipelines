package org.gbif.pipelines.core.converters;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.Issues;

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
public class EventJsonConverter {

  private static final Set<String> EXCLUDE_ALL = Collections.emptySet();
  private static final Set<String> INCLUDE_EXT_ALL = Collections.emptySet();

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final LongFunction<LocalDateTime> DATE_FN =
      l -> LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));

  private final GenericJsonConverter.GenericJsonConverterBuilder builder =
      GenericJsonConverter.builder()
          .skipKey(Indexing.MACHINE_TAGS)
          .skipKey(Indexing.CREATED)
          .converter(ExtendedRecord.class, getExtendedRecordConverter())
          .converter(IdentifierRecord.class, geIdentifierRecordConverter())
          .converter(EventCoreRecord.class, geEventCoreRecordConverter());

  @Builder.Default private boolean skipIssues = false;

  @Builder.Default private boolean skipId = true;

  @Singular private List<SpecificRecordBase> records;

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * the new ES record
   */
  public static ObjectNode toJson(SpecificRecordBase... records) {
    return EventJsonConverter.builder().records(Arrays.asList(records)).build().toJson();
  }

  /**
   * Converts all {@link SpecificRecordBase} (created from AVRO schemas) into json object, suited to
   * a partial ES record update
   */
  public static ObjectNode toPartialJson(SpecificRecordBase... records) {
    return EventJsonConverter.builder()
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

      if (!skipId) {
        jc.addJsonTextFieldNoCheck(Indexing.ID, ecr.getId());
      }

      // Fields as a common view - "key": "value"
      jc.addCommonFields(record);
    };
  }
}
