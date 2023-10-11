package org.gbif.pipelines.fragmenter.record;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.fragmenter.common.RawRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.OccurrenceRecord;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceRecordConverter {

  public static List<RawRecord> convert(
      HBaseLockingKey keygenService,
      Predicate<String> validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      boolean generateIdIfAbsent,
      List<OccurrenceRecord> recordUnitList) {

    return recordUnitList.stream()
        .map(
            r ->
                convert(
                    keygenService, validator, useTriplet, useOccurrenceId, generateIdIfAbsent, r))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  public static Optional<RawRecord> convert(
      HBaseLockingKey keygenService,
      Predicate<String> validator,
      boolean useTriplet,
      boolean useOccurrenceId,
      boolean generateIdIfAbsent,
      OccurrenceRecord or) {

    Optional<Long> key = Optional.of(Keygen.getErrorKey());
    try {
      key = Keygen.getKey(keygenService, useTriplet, useOccurrenceId, generateIdIfAbsent, or);
    } catch (RuntimeException ex) {
      log.error(ex.getMessage(), ex);
    }

    if (!key.isPresent()
        || Keygen.getErrorKey().equals(key.get())
        || !validator.test(key.toString())) {
      return Optional.empty();
    }

    return Optional.of(RawRecord.create(Keygen.getSaltedKey(key.get()), or.getStringRecord()));
  }
}
