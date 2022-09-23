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
import org.gbif.pipelines.fragmenter.common.RawRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.OccurrenceRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceRecordConverter {

  public static Map<String, RawRecord> convert(
      HBaseLockingKeyService keygenService,
      UniquenessValidator validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      boolean generateIfAbsent,
      List<OccurrenceRecord> recordUnitList) {

    Function<OccurrenceRecord, String> keyFn =
        ru -> {
          Optional<Long> key = Optional.of(Keygen.getErrorKey());
          try {
            key = Keygen.getKey(keygenService, useTriplet, useOccurrenceId, generateIfAbsent, ru);
          } catch (RuntimeException ex) {
            log.error(ex.getMessage(), ex);
          }
          if (!key.isPresent()
              || Keygen.getErrorKey().equals(key.get())
              || !validator.isUnique(key.toString())) {
            return Keygen.getErrorKey().toString();
          }
          return Keygen.getSaltedKey(key.get());
        };

    Function<OccurrenceRecord, RawRecord> valueFn = occ -> RawRecord.create(occ.toStringRecord());

    Map<String, RawRecord> result =
        recordUnitList.stream().collect(Collectors.toMap(keyFn, valueFn, (s, s2) -> s));

    result.remove(Keygen.getErrorKey().toString());
    return result;
  }
}
