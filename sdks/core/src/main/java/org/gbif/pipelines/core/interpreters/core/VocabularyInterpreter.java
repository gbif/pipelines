package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.EventsUtils.*;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractListValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyInterpreter {

  private static final Pattern TYPE_STATUS_SEPARATOR =
      Pattern.compile("^(.+) (OF|FOR) ", Pattern.CASE_INSENSITIVE);

  /** Values taken from <a href="https://github.com/gbif/vocabulary/issues/87">here</a> */
  private static final Set<String> SUSPECTED_TYPE_STATUS_VALUES =
      Set.of("?", "possible", "possibly", "potential", "maybe", "perhaps");

  /** {@link DwcTerm#lifeStage} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.lifeStage, vocabularyService).ifPresent(br::setLifeStage);
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretEstablishmentMeans(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.establishmentMeans, vocabularyService)
            .ifPresent(br::setEstablishmentMeans);
  }

  /** {@link DwcTerm#degreeOfEstablishment} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretDegreeOfEstablishment(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.degreeOfEstablishment, vocabularyService)
            .ifPresent(br::setDegreeOfEstablishment);
  }

  /** {@link DwcTerm#pathway} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretPathway(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.pathway, vocabularyService).ifPresent(br::setPathway);
  }

  /** {@link DwcTerm#pathway} interpretation. */
  public static BiConsumer<ExtendedRecord, EventCoreRecord> interpretEventType(
      VocabularyService vocabularyService) {
    return (er, ecr) ->
        ecr.setEventType(
            interpretVocabulary(er, DwcTerm.eventType, vocabularyService)
                .orElseGet(
                    () ->
                        interpretVocabulary(
                                DwcTerm.eventType, DEFAULT_EVENT_TYPE, vocabularyService)
                            .orElse(
                                VocabularyConceptFactory.createConcept(
                                    DEFAULT_EVENT_TYPE,
                                    Collections.emptyList(),
                                    Collections.emptyMap()))));
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretTypeStatus(
      VocabularyService vocabularyService) {
    return (er, br) ->
        extractListValue(er, DwcTerm.typeStatus).stream()
            .map(
                v -> {
                  Matcher m = TYPE_STATUS_SEPARATOR.matcher(v);
                  return m.find() ? m.group(1) : v;
                })
            .forEach(
                value ->
                    interpretVocabulary(
                            DwcTerm.typeStatus,
                            value,
                            vocabularyService,
                            v -> {
                              if (SUSPECTED_TYPE_STATUS_VALUES.stream()
                                  .anyMatch(sts -> v.toLowerCase().contains(sts))) {
                                addIssue(br, OccurrenceIssue.SUSPECTED_TYPE);
                              } else {
                                addIssue(br, OccurrenceIssue.TYPE_STATUS_INVALID);
                              }
                            })
                        .ifPresent(
                            v -> {
                              if (br.getTypeStatus() == null) {
                                br.setTypeStatus(new ArrayList<>());
                              }
                              br.getTypeStatus().add(v);
                            }));
  }

  /** {@link DwcTerm#sex} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretSex(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.sex, vocabularyService).ifPresent(br::setSex);
  }

  private static Optional<VocabularyConcept> interpretVocabulary(
      ExtendedRecord er, Term term, VocabularyService vocabularyService) {
    return interpretVocabulary(term, extractNullAwareValue(er, term), vocabularyService, null);
  }

  static Optional<VocabularyConcept> interpretVocabulary(
      Term term, String value, VocabularyService vocabularyService) {
    return interpretVocabulary(term, value, vocabularyService, null);
  }

  private static Optional<VocabularyConcept> interpretVocabulary(
      ExtendedRecord er, Term term, VocabularyService vocabularyService, Consumer<String> issueFn) {
    return interpretVocabulary(term, extractNullAwareValue(er, term), vocabularyService, issueFn);
  }

  /** {@link DwcTerm#degreeOfEstablishment} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretOccurrenceStatus(
      VocabularyService vocabularyService) {
    return (er, br) ->
        interpretVocabulary(er, DwcTerm.occurrenceStatus, vocabularyService)
            .ifPresent(value -> br.setOccurrenceStatus(value.getConcept()));
  }

  public static Optional<VocabularyConcept> interpretVocabulary(
      Term term, String value, VocabularyService vocabularyService, Consumer<String> issueFn) {
    if (vocabularyService == null) {
      return Optional.empty();
    }

    if (value != null) {
      Optional<VocabularyConcept> result =
          vocabularyService
              .get(term)
              .flatMap(lookup -> Optional.of(value).flatMap(lookup::lookup))
              .map(VocabularyConceptFactory::createConcept);
      if (result.isEmpty() && issueFn != null) {
        issueFn.accept(value);
      }
      return result;
    }

    return Optional.empty();
  }
}
