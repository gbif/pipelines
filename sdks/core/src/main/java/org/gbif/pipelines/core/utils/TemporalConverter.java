package org.gbif.pipelines.core.utils;

import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.io.avro.EventDate;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalConverter {

  public static Optional<TemporalAccessor> from(Integer year, Integer month, Integer day) {
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

  public static Function<EventDate, String> getEventDateToStringFn() {
    return eventDate ->
        eventDate.getGte().equals(eventDate.getLte())
            ? eventDate.getGte()
            : eventDate.getGte() + "/" + eventDate.getLte();
  }
}
