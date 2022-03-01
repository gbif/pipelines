package org.gbif.pipelines.fragmenter.record;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.fragmenter.common.Keygen;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceRecordConverter {

  public static Map<String, String> convert(
      HBaseLockingKeyService keygenService,
      UniquenessValidator validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      List<OccurrenceRecord> recordUnitList) {

    Function<OccurrenceRecord, String> keyFn =
        ru -> {
          Long key = Keygen.getErrorKey();
          try {
            key = Keygen.getKey(keygenService, useTriplet, useOccurrenceId, ru);
          } catch (RuntimeException ex) {
            log.error(ex.getMessage(), ex);
          }
          if (Keygen.getErrorKey().equals(key) || !validator.isUnique(key.toString())) {
            return Keygen.getErrorKey().toString();
          }
          return Keygen.getSaltedKey(key);
        };

    Function<OccurrenceRecord, String> valueFn = OccurrenceRecord::toStringRecord;

    Map<String, String> result =
        recordUnitList.stream().collect(Collectors.toMap(keyFn, valueFn, (s, s2) -> s));

    result.remove(Keygen.getErrorKey().toString());
    return result;
  }
}
