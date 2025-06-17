package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.model.BasicRecord;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.Parent;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.interpreters.model.EventCoreRecord;

/**
 * Interpreting function that receives a Record instance and applies an interpretation to
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoreInterpreter {

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(
          ExtendedRecord er, BasicRecord br, Consumer<String> consumer) {
    String value = er.extractValue(DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        consumer.accept(parseResult.toString());
      } else {
        br.addIssue(REFERENCES_URI_INVALID);
      }
    }
  }

  /** {@link DwcTerm#sampleSizeValue} interpretation. */
  public static void interpretSampleSizeValue(ExtendedRecord er, Consumer<Double> consumer) {
    er.extractOptValue(DwcTerm.sampleSizeValue)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(consumer);
  }

  /** {@link DwcTerm#sampleSizeUnit} interpretation. */
  public static void interpretSampleSizeUnit(ExtendedRecord er, Consumer<String> consumer) {
    er.extractOptValue(DwcTerm.sampleSizeUnit).map(String::trim).ifPresent(consumer);
  }

  /** {@link DcTerm#license} interpretation. */
  public static void interpretLicense(ExtendedRecord er, Consumer<String> consumer) {
    String license =
          er.extractOptValue(DcTerm.license)
            .map(CoreInterpreter::getLicense)
            .map(License::name)
            .orElse(License.UNSPECIFIED.name());

    consumer.accept(license);
  }

  /** {@link DwcTerm#datasetID} interpretation. */
  public static void interpretDatasetID(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = er.extractListValue(DwcTerm.datasetID);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#datasetName} interpretation. */
  public static void interpretDatasetName(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = er.extractListValue(DwcTerm.datasetName);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#parentEventID} interpretation. */
  public static void interpretParentEventID(ExtendedRecord er, Consumer<String> consumer) {
    er.extractNullAwareOptValue(DwcTerm.parentEventID).ifPresent(consumer);
  }

  public static BiConsumer<ExtendedRecord, EventCoreRecord> interpretLineages(
      Map<String, Map<String, String>> erWithParents,
      VocabularyService vocabularyService,
      Function<Void, Parent> createParentFn
    ) {

    return (er, evr) -> {
      String parentEventID = er.extractValue(DwcTerm.parentEventID);

      if (parentEventID == null) {
        return;
      }

      // Users can use the same eventId for parentEventID creating infinite loop
      if (infinitLoopCheck(parentEventID, erWithParents)) {
        evr.addIssue("EVENT_ID_TO_PARENT_ID_LOOPING_ISSUE");
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

        Parent parent = createParentFn.apply(null);
        parent.setId(parentEventID);
        VocabularyInterpreter.interpretVocabulary(
                DwcTerm.eventType, parentValues.get(DwcTerm.eventType.name()), vocabularyService)
            .ifPresent(c -> parent.setEventType(c.getConcept()));

        // allow the raw event type value through if not matched to vocab
        // this is useful as vocab is a WIP
        if (parent.getEventType() == null) {
          parent.setEventType(parentValues.get(DwcTerm.eventType.name()));
        }

        parent.setOrder(order++);
        parents.add(parent);

        parentEventID = parentValues.get(DwcTerm.parentEventID.name());
      }

      evr.setParentsLineage(parents);
    };
  }

  /** {@link DwcTerm#samplingProtocol} interpretation. */
  public static void interpretSamplingProtocol(ExtendedRecord er, Consumer<List<String>> consumer) {
    List<String> list = er.extractListValue(DwcTerm.samplingProtocol);
    if (!list.isEmpty()) {
      consumer.accept(list);
    }
  }

  /** {@link DwcTerm#locationID} interpretation. */
  public static void interpretLocationID(ExtendedRecord er, Consumer<String> consumer) {
    er.extractOptValue(DwcTerm.locationID).ifPresent(consumer);
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
