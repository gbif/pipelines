package org.gbif.pipelines.core.interpreters.core;

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
import org.gbif.vocabulary.model.Concept;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

public class DynamicPropertiesInterpreter {

  public static void interpretHasTissue(ExtendedRecord er, BasicRecord br) {
    String value = extractNullAwareValue(er, DwcTerm.preparations);
    DynamicProperty.Builder builder = DynamicProperty.newBuilder().setType(Type.BOOLEAN);
    if (value == null || value.isEmpty()) {
      builder.setValue(Boolean.FALSE.toString());
    } else {
      boolean any = TissueParser.hasTissue(value);
      builder.setValue(Boolean.valueOf(any).toString());
    }
    br.getDynamicProperties().put(Key.HAS_TISSUE, builder.build());
  }

  public static void interpretSex(ExtendedRecord er, BasicRecord br) {
    if (br.getSex() == null) {
      String value = extractNullAwareValue(er, DwcTerm.dynamicProperties);

      Consumer<ParseResult<Sex>> fn =
          parseResult -> {
            if (parseResult.isSuccessful()) {
              br.setSex(parseResult.getPayload().name());
            }
          };

      SexParser.parse(value).ifPresent(r -> VocabularyParser.sexParser().parse(r, fn));
    }
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretLifeStage(
      Function<String, Optional<Concept>> vocabularyLookupFn) {
    return (er, br) -> {
      if (vocabularyLookupFn == null || br.getLifeStage() != null) {
        return;
      }

      LifeStageParser.parse(extractNullAwareValue(er, DwcTerm.dynamicProperties))
          .flatMap(vocabularyLookupFn)
          .map(Concept::getName)
          .ifPresent(br::setLifeStage);
    };
  }
}
