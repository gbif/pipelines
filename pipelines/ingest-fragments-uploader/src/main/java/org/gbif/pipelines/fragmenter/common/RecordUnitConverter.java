package org.gbif.pipelines.fragmenter.common;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RecordUnitConverter {

  public static Map<String, String> convert(HBaseLockingKeyService keygenService, UniquenessValidator validator,
      boolean useTriplet, boolean useOccurrenceId, List<RecordUnit> recordUnitList) {

    Function<RecordUnit, String> keyFn = ru -> {
      Long key = Keygen.getKey(keygenService, useTriplet, useOccurrenceId, ru);
      key = validator.isUnique(key.toString()) ? key : Keygen.getErrorKey();
      return Keygen.getSaltedKey(key);
    };

    Function<RecordUnit, String> valueFn = RecordUnit::toString;

    Map<String, String> result = recordUnitList.stream().collect(Collectors.toMap(keyFn, valueFn, (s, s2) -> s));

    result.remove(Keygen.getErrorKey().toString());
    return result;
  }
}
