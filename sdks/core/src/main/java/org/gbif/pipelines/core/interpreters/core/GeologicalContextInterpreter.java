package org.gbif.pipelines.core.interpreters.core;

import java.util.Collections;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GeologicalContext;
import org.gbif.pipelines.io.avro.VocabularyConcept;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GeologicalContextInterpreter {

  /** {@link DwcTerm#earliestEonOrLowestEonothem} interpretation. */
  public static void interpretEarliestEonOrLowestEonothem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.earliestEonOrLowestEonothem,
        GeologicalContext::setEarliestEonOrLowestEonothem);
  }

  /** {@link DwcTerm#latestEonOrHighestEonothem} interpretation. */
  public static void interpretLatestEonOrHighestEonothem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.latestEonOrHighestEonothem,
        GeologicalContext::setLatestEonOrHighestEonothem);
  }

  /** {@link DwcTerm#earliestEraOrLowestErathem} interpretation. */
  public static void interpretEarliestEraOrLowestErathem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.earliestEraOrLowestErathem,
        GeologicalContext::setEarliestEraOrLowestErathem);
  }

  /** {@link DwcTerm#latestEraOrHighestErathem} interpretation. */
  public static void interpretLatestEraOrHighestErathem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er, br, DwcTerm.latestEraOrHighestErathem, GeologicalContext::setLatestEraOrHighestErathem);
  }

  /** {@link DwcTerm#earliestPeriodOrLowestSystem} interpretation. */
  public static void interpretEarliestPeriodOrLowestSystem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.earliestPeriodOrLowestSystem,
        GeologicalContext::setEarliestPeriodOrLowestSystem);
  }

  /** {@link DwcTerm#latestPeriodOrHighestSystem} interpretation. */
  public static void interpretLatestPeriodOrHighestSystem(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.latestPeriodOrHighestSystem,
        GeologicalContext::setLatestPeriodOrHighestSystem);
  }

  /** {@link DwcTerm#earliestEpochOrLowestSeries} interpretation. */
  public static void interpretEarliestEpochOrLowestSeries(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.earliestEpochOrLowestSeries,
        GeologicalContext::setEarliestEpochOrLowestSeries);
  }

  /** {@link DwcTerm#latestEpochOrHighestSeries} interpretation. */
  public static void interpretLatestEpochOrHighestSeries(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er,
        br,
        DwcTerm.latestEpochOrHighestSeries,
        GeologicalContext::setLatestEpochOrHighestSeries);
  }

  /** {@link DwcTerm#earliestAgeOrLowestStage} interpretation. */
  public static void interpretEarliestAgeOrLowestStage(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er, br, DwcTerm.earliestAgeOrLowestStage, GeologicalContext::setEarliestAgeOrLowestStage);
  }

  /** {@link DwcTerm#latestAgeOrHighestStage} interpretation. */
  public static void interpretLatestAgeOrHighestStage(ExtendedRecord er, BasicRecord br) {
    interpretVocabularyConceptTerm(
        er, br, DwcTerm.latestAgeOrHighestStage, GeologicalContext::setLatestAgeOrHighestStage);
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
      BiConsumer<GeologicalContext, String> setFn) {
    ModelUtils.extractNullAwareOptValue(er, term)
        .ifPresent(
            v -> {
              GeologicalContext gx = br.getGeologicalContext();
              if (gx == null) {
                gx = new GeologicalContext();
                br.setGeologicalContext(gx);
              }
              setFn.accept(gx, v);
            });
  }

  private static void interpretVocabularyConceptTerm(
      ExtendedRecord er,
      BasicRecord br,
      DwcTerm term,
      BiConsumer<GeologicalContext, VocabularyConcept> setFn) {
    ModelUtils.extractNullAwareOptValue(er, term)
        .ifPresent(
            v -> {
              GeologicalContext gx = br.getGeologicalContext();
              if (gx == null) {
                gx = new GeologicalContext();
                br.setGeologicalContext(gx);
              }
              VocabularyConcept vc =
                  VocabularyConcept.newBuilder()
                      .setConcept(v)
                      .setLineage(Collections.singletonList(v))
                      .build();
              setFn.accept(gx, vc);
            });
  }
}
