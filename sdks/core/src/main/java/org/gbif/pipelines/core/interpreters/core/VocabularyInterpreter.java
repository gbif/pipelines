package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyInterpreter {

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
        interpretVocabulary(er, DwcTerm.eventType, vocabularyService).ifPresent(ecr::setEventType);
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  private static Optional<VocabularyConcept> interpretVocabulary(
      ExtendedRecord er, Term term, VocabularyService vocabularyService) {
    return interpretVocabulary(term, extractNullAwareValue(er, term), vocabularyService);
  }

  static Optional<VocabularyConcept> interpretVocabulary(
      Term term, String value, VocabularyService vocabularyService) {

    if (vocabularyService != null) {
      return vocabularyService
          .get(term)
          .flatMap(lookup -> Optional.ofNullable(value).flatMap(lookup::lookup))
          .map(VocabularyUtils::getConcept);
    }
    return Optional.empty();
  }
}
