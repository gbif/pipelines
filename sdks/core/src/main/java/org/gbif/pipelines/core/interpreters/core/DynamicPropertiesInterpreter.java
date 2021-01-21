package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.gbif.api.vocabulary.Sex;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.dynamic.LifeStageParser;
import org.gbif.pipelines.core.parsers.dynamic.SexParser;
import org.gbif.pipelines.core.parsers.dynamic.TissueParser;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.vocabulary.lookup.LookupConcept;

public class DynamicPropertiesInterpreter {

  public static void interpretHasTissue(ExtendedRecord er, BasicRecord br) {
    extractNullAwareOptValue(er, DwcTerm.preparations)
        .ifPresent(
            v -> {
              DynamicProperty.Builder builder = DynamicProperty.newBuilder().setType(Type.BOOLEAN);
              boolean any = TissueParser.hasTissue(v);
              builder.setValue(Boolean.valueOf(any).toString());
              br.getDynamicProperties().put(Key.HAS_TISSUE, builder.build());
            });
  }

  public static void interpretSex(ExtendedRecord er, BasicRecord br) {
    if (br.getSex() != null) {
      return;
    }
    extractNullAwareOptValue(er, DwcTerm.dynamicProperties)
        .ifPresent(
            v -> {
              Consumer<ParseResult<Sex>> fn =
                  parseResult -> {
                    if (parseResult.isSuccessful()) {
                      br.setSex(parseResult.getPayload().name());
                    }
                  };

              SexParser.parse(v).ifPresent(r -> VocabularyParser.sexParser().parse(r, fn));
            });
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
