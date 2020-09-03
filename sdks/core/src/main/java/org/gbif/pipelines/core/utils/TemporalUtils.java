package org.gbif.pipelines.core.utils;

import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalUtils {

  public static Optional<Temporal> getTemporal(Integer year, Integer month, Integer day) {
    try {
      if (year != null && month != null && day != null) {
        return Optional.of(LocalDate.of(year, month, day));
      }
      if (year != null && month != null) {
        return Optional.of(YearMonth.of(year, month));
      }
      if (year != null) {
        return Optional.of(Year.of(year));
      }
    } catch (RuntimeException ex) {
      log.warn(ex.getLocalizedMessage());
    }
    return Optional.empty();
  }
}
