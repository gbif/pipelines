package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.dwc.terms.DwcTerm.earliestEraOrLowestErathem;
import static org.gbif.dwc.terms.DwcTerm.latestEraOrHighestErathem;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;
import org.gbif.pipelines.core.interpreters.model.BasicRecord;
import org.gbif.pipelines.core.interpreters.model.GeologicalContext;
import org.gbif.pipelines.core.interpreters.model.VocabularyConcept;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.jetbrains.annotations.Nullable;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GeologicalContextInterpreter {

  public static final String RANK_TAG = "rank";
  public static final String START_AGE_TAG = "startage";
  public static final String END_AGE_TAG = "endage";
  private static final String TAG_SEPARATOR = ":";
  private static final String EON = "eon";
  private static final String ERA = "era";
  private static final String PERIOD = "period";
  private static final String EPOCH = "epoch";
  private static final String AGE = "age";
  private static final List<String> RANK_HIERARCHY = Arrays.asList(EON, ERA, PERIOD, EPOCH, AGE);

  /**
   * {@link DwcTerm#earliestEonOrLowestEonothem}, {@link DwcTerm#latestEonOrHighestEonothem}, {@link
   * DwcTerm#earliestEraOrLowestErathem},{@link DwcTerm#latestEraOrHighestErathem}, {@link
   * DwcTerm#earliestPeriodOrLowestSystem}, {@link DwcTerm#latestPeriodOrHighestSystem}, {@link
   * DwcTerm#earliestEpochOrLowestSeries}, {@link DwcTerm#latestEpochOrHighestSeries}, {@link
   * DwcTerm#latestAgeOrHighestStage} and {@link DwcTerm#earliestAgeOrLowestStage} interpretation.
   */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretChronostratigraphy(
      VocabularyService vocabularyService) {
    return (er, br) -> {
      GeologicalContext gx = br.getGeologicalContext();
      if (gx == null) {
        gx = br.createGeologicalContext();
        br.setGeologicalContext(gx);
      }

      Range eonRange =
          interpretTermPair(
              er,
              br,
              TermPair.of(
                  null,
                  DwcTerm.earliestEonOrLowestEonothem,
                  DwcTerm.latestEonOrHighestEonothem,
                  GeologicalContext::setEarliestEonOrLowestEonothem,
                  GeologicalContext::setLatestEonOrHighestEonothem),
              vocabularyService);
      Range eraRange =
          interpretTermPair(
              er,
              br,
              TermPair.of(
                  eonRange,
                  earliestEraOrLowestErathem,
                  latestEraOrHighestErathem,
                  GeologicalContext::setEarliestEraOrLowestErathem,
                  GeologicalContext::setLatestEraOrHighestErathem),
              vocabularyService);
      Range periodRange =
          interpretTermPair(
              er,
              br,
              TermPair.of(
                  chooseFirst(eraRange, eonRange).orElse(null),
                  DwcTerm.earliestPeriodOrLowestSystem,
                  DwcTerm.latestPeriodOrHighestSystem,
                  GeologicalContext::setEarliestPeriodOrLowestSystem,
                  GeologicalContext::setLatestPeriodOrHighestSystem),
              vocabularyService);
      Range epochRange =
          interpretTermPair(
              er,
              br,
              TermPair.of(
                  chooseFirst(periodRange, eraRange, eonRange).orElse(null),
                  DwcTerm.earliestEpochOrLowestSeries,
                  DwcTerm.latestEpochOrHighestSeries,
                  GeologicalContext::setEarliestEpochOrLowestSeries,
                  GeologicalContext::setLatestEpochOrHighestSeries),
              vocabularyService);
      Range ageRange =
          interpretTermPair(
              er,
              br,
              TermPair.of(
                  chooseFirst(epochRange, periodRange, eraRange, eonRange).orElse(null),
                  DwcTerm.earliestAgeOrLowestStage,
                  DwcTerm.latestAgeOrHighestStage,
                  GeologicalContext::setEarliestAgeOrLowestStage,
                  GeologicalContext::setLatestAgeOrHighestStage),
              vocabularyService);

      chooseFirst(ageRange, epochRange, periodRange, eraRange, eonRange)
          .ifPresent(
              r -> {
                if (r.start != null) {
                  br.getGeologicalContext().setStartAge(r.start);
                }
                if (r.end != null) {
                  br.getGeologicalContext().setEndAge(r.end);
                }
              });
    };
  }

  /** {@link DwcTerm#lowestBiostratigraphicZone} interpretation. */
  public static void interpretLowestBiostratigraphicZone(ExtendedRecord er, BasicRecord br) {
    interpretTerm(
        er,
        br,
        DwcTerm.lowestBiostratigraphicZone,
        GeologicalContext::setLowestBiostratigraphicZone);
  }

  /** {@link DwcTerm#highestBiostratigraphicZone} interpretation. */
  public static void interpretHighestBiostratigraphicZone(ExtendedRecord er, BasicRecord br) {
    interpretTerm(
        er,
        br,
        DwcTerm.highestBiostratigraphicZone,
        GeologicalContext::setHighestBiostratigraphicZone);
  }

  /** {@link DwcTerm#group} interpretation. */
  public static void interpretGroup(ExtendedRecord er, BasicRecord br) {
    interpretTerm(er, br, DwcTerm.group, GeologicalContext::setGroup);
  }

  /** {@link DwcTerm#formation} interpretation. */
  public static void interpretFormation(ExtendedRecord er, BasicRecord br) {
    interpretTerm(er, br, DwcTerm.formation, GeologicalContext::setFormation);
  }

  /** {@link DwcTerm#member} interpretation. */
  public static void interpretMember(ExtendedRecord er, BasicRecord br) {
    interpretTerm(er, br, DwcTerm.member, GeologicalContext::setMember);
  }

  /** {@link DwcTerm#bed} interpretation. */
  public static void interpretBed(ExtendedRecord er, BasicRecord br) {
    interpretTerm(er, br, DwcTerm.bed, GeologicalContext::setBed);
  }

  private static void interpretTerm(
      ExtendedRecord er,
      BasicRecord br,
      DwcTerm term,
      BiConsumer<GeologicalContext, String> setFn
      ) {
    er.extractNullAwareOptValue(term)
        .ifPresent(
            v -> {
              GeologicalContext gx = br.getGeologicalContext();
              if (gx == null) {
                gx = br.createGeologicalContext();
                br.setGeologicalContext(gx);
              }
              setFn.accept(gx, v);
            });
  }

  private static Optional<Range> chooseFirst(Range... ranges) {
    for (Range r : ranges) {
      if (!r.isEmpty()) {
        return Optional.of(r);
      }
    }
    return Optional.empty();
  }

  private static Range interpretTermPair(
          ExtendedRecord er, BasicRecord br, TermPair termPair, VocabularyService vocabularyService) {
    Optional<VocabularyConcept> earliestVocabularyConceptOpt =
            er.extractNullAwareOptValue(termPair.earliestTerm)
            .flatMap(v -> vocabularyService.get(termPair.earliestTerm).flatMap(l -> l.lookup(v)))
            .flatMap(l -> getVocabularyConcept(l, termPair.earliestTerm, br));

    Optional<VocabularyConcept> latestVocabularyConceptOpt =
            er.extractNullAwareOptValue(termPair.latestTerm)
            .flatMap(v -> vocabularyService.get(termPair.latestTerm).flatMap(l -> l.lookup(v)))
            .flatMap(l -> getVocabularyConcept(l, termPair.earliestTerm, br));

    if (earliestVocabularyConceptOpt.isEmpty() && latestVocabularyConceptOpt.isEmpty()) {
      return Range.empty();
    }

    Float earliestStartAge = null;
    Float earliestEndAge = null;
    if (earliestVocabularyConceptOpt.isPresent()) {
      earliestStartAge = getAge(earliestVocabularyConceptOpt.get(), START_AGE_TAG);
      earliestEndAge = getAge(earliestVocabularyConceptOpt.get(), END_AGE_TAG);
    }

    Float latestStartAge = null;
    Float latestEndAge = null;
    if (latestVocabularyConceptOpt.isPresent()) {
      latestStartAge = getAge(latestVocabularyConceptOpt.get(), START_AGE_TAG);
      latestEndAge = getAge(latestVocabularyConceptOpt.get(), END_AGE_TAG);
    }

    if ((latestStartAge != null && earliestStartAge != null && latestStartAge > earliestStartAge)
        || (latestEndAge != null && earliestEndAge != null && latestEndAge > earliestEndAge)) {
      Optional.ofNullable(getTermData(termPair.earliestTerm).invalidRange)
          .ifPresent(i -> br.addIssue(i));
      return Range.empty();
    }

    GeologicalContext gx = br.getGeologicalContext();
    earliestVocabularyConceptOpt.ifPresent(
        vocabularyConcept -> termPair.setEarliestFn.accept(gx, vocabularyConcept));
    latestVocabularyConceptOpt.ifPresent(
        vocabularyConcept -> termPair.setLatestFn.accept(gx, vocabularyConcept));

    Range termRange =
        Range.of(
            earliestStartAge != null ? earliestStartAge : latestStartAge,
            latestEndAge != null ? latestEndAge : earliestEndAge);

    if (termPair.parentRange != null && !termPair.parentRange.isEmpty()) {
      if ((termPair.parentRange.start != null && termRange.start > termPair.parentRange.start)
          || (termPair.parentRange.end != null && termRange.end < termPair.parentRange.end)) {
        Optional.ofNullable(getTermData(termPair.earliestTerm).parentMismatch)
            .ifPresent(i -> br.addIssue(i));
      }
    }

    return termRange;
  }

  @Nullable
  private static Float getAge(VocabularyConcept earliestVocabularyConcept, String startAgeTag) {
    return earliestVocabularyConcept.getTags().stream()
        .filter(t -> t.getName().equals(startAgeTag))
        .map(t -> Float.parseFloat(t.getValue()))
        .findFirst()
        .orElse(null);
  }

  private static Optional<VocabularyConcept> getVocabularyConcept(
      LookupConcept lookupConcept, DwcTerm term, BasicRecord br) {
    Map<String, String> tagsMap = getTagsMap(lookupConcept.getTags());
    String termRank = getTermData(term).rank;
    if (tagsMap.get(RANK_TAG).equals(termRank)) {
      return Optional.of(VocabularyConceptFactory.createConcept(lookupConcept, tagsMap));
    } else {
      // rank doesn't match
      if (RANK_HIERARCHY.indexOf(tagsMap.get(RANK_TAG)) > RANK_HIERARCHY.indexOf(termRank)) {
        Optional<LookupConcept.Parent> parentOpt =
            lookupConcept.getParents().stream()
                .filter(p -> getTagsMap(p.getTags()).get(RANK_TAG).equals(termRank))
                .findFirst();

        if (parentOpt.isPresent()) {
          Optional.ofNullable(getTermData(term).inferredFromParent).ifPresent(br::addIssue);

          LookupConcept.Parent parent = parentOpt.get();
          List<LookupConcept.Parent> allParents = lookupConcept.getParents();
          List<LookupConcept.Parent> filteredParents =
              allParents.subList(allParents.indexOf(parent) + 1, allParents.size());
          return Optional.of(
              VocabularyConceptFactory.createConcept(
                  parent.getName(), filteredParents, getTagsMap(parent.getTags())));
        }
      } else {
        Optional.ofNullable(getTermData(term).rankMismatch).ifPresent(br::addIssue);
      }
    }
    return Optional.empty();
  }

  private static Map<String, String> getTagsMap(List<String> tags) {
    return tags.stream()
        .filter(t -> t.contains(TAG_SEPARATOR))
        .map(t -> t.split(TAG_SEPARATOR))
        .filter(v -> v.length == 2)
        .filter(
            v ->
                Arrays.asList(RANK_TAG, START_AGE_TAG, END_AGE_TAG)
                    .contains(v[0].toLowerCase().trim()))
        .collect(Collectors.toMap(v -> v[0].toLowerCase().trim(), v -> v[1].toLowerCase().trim()));
  }

  private static TermData getTermData(DwcTerm term) {
    switch (term) {
      case earliestEonOrLowestEonothem:
      case latestEonOrHighestEonothem:
        return TermData.of(
            EON,
            null,
            OccurrenceIssue.EON_OR_EONOTHEM_RANK_MISMATCH,
            OccurrenceIssue.EON_OR_EONOTHEM_INVALID_RANGE,
            null);
      case earliestEraOrLowestErathem:
      case latestEraOrHighestErathem:
        return TermData.of(
            ERA,
            OccurrenceIssue.ERA_OR_ERATHEM_INFERRED_FROM_PARENT_RANK,
            OccurrenceIssue.ERA_OR_ERATHEM_RANK_MISMATCH,
            OccurrenceIssue.ERA_OR_ERATHEM_INVALID_RANGE,
            OccurrenceIssue.EON_OR_EONOTHEM_AND_ERA_OR_ERATHEM_MISMATCH);
      case earliestPeriodOrLowestSystem:
      case latestPeriodOrHighestSystem:
        return TermData.of(
            PERIOD,
            OccurrenceIssue.PERIOD_OR_SYSTEM_INFERRED_FROM_PARENT_RANK,
            OccurrenceIssue.PERIOD_OR_SYSTEM_RANK_MISMATCH,
            OccurrenceIssue.PERIOD_OR_SYSTEM_INVALID_RANGE,
            OccurrenceIssue.ERA_OR_ERATHEM_AND_PERIOD_OR_SYSTEM_MISMATCH);
      case earliestEpochOrLowestSeries:
      case latestEpochOrHighestSeries:
        return TermData.of(
            EPOCH,
            OccurrenceIssue.EPOCH_OR_SERIES_INFERRED_FROM_PARENT_RANK,
            OccurrenceIssue.EPOCH_OR_SERIES_RANK_MISMATCH,
            OccurrenceIssue.EPOCH_OR_SERIES_INVALID_RANGE,
            OccurrenceIssue.PERIOD_OR_SYSTEM_AND_EPOCH_OR_SERIES_MISMATCH);
      case earliestAgeOrLowestStage:
      case latestAgeOrHighestStage:
        return TermData.of(
            AGE,
            OccurrenceIssue.AGE_OR_STAGE_INFERRED_FROM_PARENT_RANK,
            OccurrenceIssue.AGE_OR_STAGE_RANK_MISMATCH,
            OccurrenceIssue.AGE_OR_STAGE_INVALID_RANGE,
            OccurrenceIssue.EPOCH_OR_SERIES_AND_AGE_OR_STAGE_MISMATCH);
      default:
        return TermData.of(null, null, null, null, null);
    }
  }

  @AllArgsConstructor(staticName = "of")
  private static class TermPair {
    GeologicalContextInterpreter.Range parentRange;
    DwcTerm earliestTerm;
    DwcTerm latestTerm;
    BiConsumer<GeologicalContext, VocabularyConcept> setEarliestFn;
    BiConsumer<GeologicalContext, VocabularyConcept> setLatestFn;
  }

  @AllArgsConstructor(staticName = "of")
  private static class Range {
    Float start;
    Float end;

    static Range empty() {
      return Range.of(null, null);
    }

    boolean isEmpty() {
      return start == null && end == null;
    }
  }

  @AllArgsConstructor(staticName = "of")
  private static class TermData {
    String rank;
    OccurrenceIssue inferredFromParent;
    OccurrenceIssue rankMismatch;
    OccurrenceIssue invalidRange;
    OccurrenceIssue parentMismatch;
  }
}
