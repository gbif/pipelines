package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;

import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.vertnet.LifeStageParser;
import org.gbif.pipelines.core.parsers.vertnet.SexParser;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyConceptFactory;
import org.gbif.pipelines.core.interpreters.model.BasicRecord;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DynamicPropertiesInterpreter {

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretSex(
      VocabularyService vocabularyService) {
    return (er, br) -> {
      if (br.getSex() == null) {
        vocabularyService
            .get(DwcTerm.sex)
            .flatMap(
                lookup ->
                        er.extractNullAwareOptValue(DwcTerm.dynamicProperties)
                        .flatMap(v -> SexParser.parse(v).flatMap(lookup::lookup)))
            .map(VocabularyConceptFactory::createConcept)
            .ifPresent(br::setSex);
      }
    };
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      VocabularyService vocabularyService) {
    return (er, br) -> {
      if (br.getLifeStage() == null) {
        vocabularyService
            .get(DwcTerm.lifeStage)
            .flatMap(
                lookup ->
                        er.extractNullAwareOptValue(DwcTerm.dynamicProperties)
                        .flatMap(v -> LifeStageParser.parse(v).flatMap(lookup::lookup)))
            .map(VocabularyConceptFactory::createConcept)
            .ifPresent(br::setLifeStage);
      }
    };
  }
}
