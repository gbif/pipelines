package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.pipelines.core.utils.EventsUtils.*;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractListValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.Parent;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoreInterpreter {

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(
      ExtendedRecord er, Issues issues, Consumer<String> consumer) {
    String value = extractValue(er, DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        consumer.accept(parseResult.toString());
      } else {
        addIssue(issues, REFERENCES_URI_INVALID);
      }
    }
  }

  /** {@link DwcTerm#sampleSizeValue} interpretation. */
  public static void interpretSampleSizeValue(ExtendedRecord er, Consumer<Double> consumer) {
    extractOptValue(er, DwcTerm.sampleSizeValue)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(consumer);
  }

  /** {@link DwcTerm#sampleSizeUnit} interpretation. */
  public static void interpretSampleSizeUnit(ExtendedRecord er, Consumer<String> consumer) {
    extractOptValue(er, DwcTerm.sampleSizeUnit).map(String::trim).ifPresent(consumer);
  }

  /** {@link DcTerm#license} interpretation. */
  public static void interpretLicense(ExtendedRecord er, Consumer<String> consumer) {
    String license =
        extractOptValue(er, DcTerm.license)
            .map(CoreInterpreter::getLicense)
            .map(License::name)
            .orElse(License.UNSPECIFIED.name());

    consumer.accept(license);
  }

  /** {@link DwcTerm#datasetID} interpretation. */
  public static void interpretDatasetID(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = extractListValue(er, DwcTerm.datasetID);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#datasetName} interpretation. */
  public static void interpretDatasetName(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = extractListValue(er, DwcTerm.datasetName);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#parentEventID} interpretation. */
  public static void interpretParentEventID(ExtendedRecord er, Consumer<String> consumer) {
    extractNullAwareOptValue(er, DwcTerm.parentEventID).ifPresent(consumer);
  }

  public static BiConsumer<ExtendedRecord, EventCoreRecord> interpretLineages(
      Map<String, Map<String, String>> erWithParents, VocabularyService vocabularyService) {
    return (er, evr) -> {
      String parentEventID = extractValue(er, DwcTerm.parentEventID);

      if (parentEventID == null) {
        return;
      }

      // Users can use the same eventId for parentEventID creating infinite loop
      if (infinitLoopCheck(parentEventID, erWithParents)) {
        addIssue(evr, "EVENT_ID_TO_PARENT_ID_LOOPING_ISSUE");
        return;
      }

      // parent event IDs
      List<Parent> parents = new ArrayList<>();
      int order = 1;
      while (parentEventID != null) {
        Map<String, String> parentValues = erWithParents.get(parentEventID);

        if (parentValues == null) {
          // case when there is no event with that parentEventID
          break;
        }

        Parent.Builder parentBuilder = Parent.newBuilder().setId(parentEventID);
        parentBuilder.setEventType(
            VocabularyInterpreter.interpretVocabulary(
                    DwcTerm.eventType,
                    parentValues.get(DwcTerm.eventType.name()),
                    vocabularyService)
                .orElseGet(
                    // allow the raw event type value through if not matched to vocab
                    // this is useful as vocab is a WIP
                    () ->
                        VocabularyInterpreter.interpretVocabulary(
                                DwcTerm.eventType, DEFAULT_EVENT_TYPE, vocabularyService)
                            .orElse(
                                VocabularyConceptFactory.createConcept(
                                    DEFAULT_EVENT_TYPE,
                                    Collections.emptyList(),
                                    Collections.emptyMap())))
                .getConcept());

        parentBuilder.setVerbatimEventType(
            Optional.ofNullable(parentValues.get(DwcTerm.eventType.name()))
                .orElse(DEFAULT_EVENT_TYPE));

        parentBuilder.setOrder(order++);
        parents.add(parentBuilder.build());

        parentEventID = parentValues.get(DwcTerm.parentEventID.name());
      }

      evr.setParentsLineage(parents);
    };
  }

  /** {@link DwcTerm#samplingProtocol} interpretation. */
  public static void interpretSamplingProtocol(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = extractListValue(er, DwcTerm.samplingProtocol);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#locationID} interpretation. */
  public static void interpretLocationID(ExtendedRecord er, Consumer<String> consumer) {
    extractOptValue(er, DwcTerm.locationID).ifPresent(consumer);
  }

  /** Some parentEventID can have looped link */
  private static boolean infinitLoopCheck(
      String initialId, Map<String, Map<String, String>> erWithParents) {

    Set<String> idSet = new HashSet<>();
    idSet.add(initialId);
    Map<String, String> nextMap = erWithParents.get(initialId);
    while (nextMap != null) {
      String nextId = nextMap.get(DwcTerm.parentEventID.name());
      if (nextId == null) {
        return false;
      } else {
        nextMap = erWithParents.get(nextId);
      }
      if (idSet.contains(nextId)) {
        return true;
      } else {
        idSet.add(nextId);
      }
    }
    return false;
  }

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }
}
