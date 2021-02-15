package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Sex;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.vertnet.LifeStageParser;
import org.gbif.pipelines.core.parsers.vertnet.SexParser;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.vocabulary.lookup.LookupConcept;

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
      Function<String, Optional<LookupConcept>> vocabularyLookupFn) {
    return (er, br) -> {
      if (vocabularyLookupFn == null || br.getLifeStage() != null) {
        return;
      }

      extractNullAwareOptValue(er, DwcTerm.dynamicProperties)
          .flatMap(v -> LifeStageParser.parse(v).flatMap(vocabularyLookupFn))
          .ifPresent(x -> BasicInterpreter.getLookupConceptConsumer(br).accept(x));
    };
  }
}
