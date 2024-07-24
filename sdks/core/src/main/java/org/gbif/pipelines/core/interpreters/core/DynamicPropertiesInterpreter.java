package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Sex;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.vertnet.LifeStageParser;
import org.gbif.pipelines.core.parsers.vertnet.SexParser;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.VocabularyUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DynamicPropertiesInterpreter {

  public static void interpretSex(ExtendedRecord er, BasicRecord br) {
    if (br.getSex() != null) {
      return;
    }

    Consumer<ParseResult<Sex>> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setSex(parseResult.getPayload().name());
          }
        };

    extractNullAwareOptValue(er, DwcTerm.dynamicProperties)
        .flatMap(SexParser::parse)
        .ifPresent(r -> VocabularyParser.sexParser().parse(r, fn));
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      VocabularyService vocabularyService) {
    return (er, br) -> {
      if (br.getLifeStage() == null) {
        vocabularyService
            .get(DwcTerm.lifeStage)
            .flatMap(
                lookup ->
                    extractNullAwareOptValue(er, DwcTerm.dynamicProperties)
                        .flatMap(v -> LifeStageParser.parse(v).flatMap(lookup::lookup)))
            .map(VocabularyUtils::getConcept)
            .ifPresent(br::setLifeStage);
      }
    };
  }
}
