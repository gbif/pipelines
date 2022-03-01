package org.gbif.pipelines.fragmenter.record;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private static final String ERROR_KEY = "-1";

  public static Map<String, String> convert(
      HBaseLockingKeyService keygenService,
      UniquenessValidator validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      List<OccurrenceRecord> recordUnitList) {

    Function<OccurrenceRecord, String> keyFn =
        ru -> {
          Optional<Long> key = Optional.empty();
          try {
            key = Keygen.getOrGenerateKey(keygenService, useTriplet, useOccurrenceId, ru);
          } catch (RuntimeException ex) {
            log.error(ex.getMessage(), ex);
          }
          if (!key.isPresent() || !validator.isUnique(key.toString())) {
            return ERROR_KEY;
          }
          return Keygen.getSaltedKey(key.get());
        };

    Function<OccurrenceRecord, String> valueFn = OccurrenceRecord::toStringRecord;

    Map<String, String> result =
        recordUnitList.stream().collect(Collectors.toMap(keyFn, valueFn, (s, s2) -> s));

    result.remove(ERROR_KEY);
    return result;
  }
}
