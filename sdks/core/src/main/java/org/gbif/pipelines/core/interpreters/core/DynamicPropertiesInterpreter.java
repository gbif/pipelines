package org.gbif.pipelines.core.interpreters.core;

import org.gbif.api.vocabulary.Sex;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Key;
import org.gbif.pipelines.common.PipelinesVariables.DynamicProperties.Type;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.dynamic.SexParser;
import org.gbif.pipelines.core.parsers.dynamic.TissueParser;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.DynamicProperty;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Consumer;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

public class DynamicPropertiesInterpreter {

  public static void interpretHasTissue(ExtendedRecord er, BasicRecord br) {
    String value = extractNullAwareValue(er, DwcTerm.preparations);
    DynamicProperty.Builder builder = DynamicProperty.newBuilder().setType(Type.BOOLEAN);
    if (value == null || value.isEmpty()) {
      builder.setValue(Boolean.FALSE.toString());
    } else {
      boolean any = TissueParser.parseHasTissue(value);
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

      SexParser.parseSex(value).ifPresent(r -> VocabularyParser.sexParser().parse(r, fn));
    }
  }
}
