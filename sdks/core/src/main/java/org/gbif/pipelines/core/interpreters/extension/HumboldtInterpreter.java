package org.gbif.pipelines.core.interpreters.extension;

import static org.gbif.pipelines.core.utils.ModelUtils.extractListValue;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.common.Strings;

import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;

@Builder(buildMethodName = "create")
@Slf4j
public class HumboldtInterpreter {

  /**
   * Interprets audubon of a {@link ExtendedRecord} and populates a {@link HumboldtRecord} with the
   * interpreted values.
   */
  public void interpret(ExtendedRecord er, HumboldtRecord hr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(hr);

    ExtensionInterpretation.Result<Humboldt> result =
        ExtensionInterpretation.extension(Extension.HUMBOLDT)
            .to(Humboldt::new)
            .map(
                EcoTerm.verbatimSiteDescriptions,
                interpretStringList(Humboldt::setVerbatimSiteDescriptions))
            .map(EcoTerm.verbatimSiteNames, interpretStringList(Humboldt::setVerbatimSiteNames))
            .map(
                EcoTerm.geospatialScopeAreaValue,
                HumboldtInterpreter::interpretGeospatialScopeAreaValue)
            .convert(er);

    hr.setHumboldtItems(result.getList());
  }

  private static BiConsumer<Humboldt, String> interpretStringList(
      BiConsumer<Humboldt, List<String>> setter) {
    return (humboldt, rawValue) -> {
      List<String> list = extractListValue(rawValue);
      if (!list.isEmpty()) {
        setter.accept(humboldt, list);
      }
    };
  }

  // TODO: post mapper GeospatialScopeAreaValue is always greater than or equal to the
  // eco:totalAreaSampledValue
  private static void interpretGeospatialScopeAreaValue(Humboldt humboldt, String rawValue) {
    Consumer<Optional<Double>> fn =
        parseResult -> {
          Double result = parseResult.orElse(null);
          if (result != null && result >= 0) {
            humboldt.setGeospatialScopeAreaValue(result.floatValue());
          } else {
            // TODO:add issue negative number
          }
        };

    SimpleTypeParser.parseDouble(rawValue, fn);
  }

  private static void interpretGeospatialScopeAreaUnit(Humboldt humboldt, String rawValue) {
    if (!Strings.isNullOrEmpty(rawValue)) {
      // TODO: parse area unit
    }
  }
}
