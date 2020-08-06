package org.gbif.pipelines.core.parsers.temporal.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalUtils {

  public static Optional<Temporal> getTemporal(
      Year year, Month month, Integer day, LocalTime time, ZoneOffset offset) {
    Temporal result = null;
    if (year != null && month != null && day != null && time != null && offset != null) {
      result = OffsetDateTime.of(LocalDate.of(year.getValue(), month, day), time, offset);
    } else if (year != null && month != null && day != null && time != null) {
      result = LocalDateTime.of(LocalDate.of(year.getValue(), month, day), time);
    } else if (year != null && month != null && day != null) {
      result = LocalDate.of(year.getValue(), month, day);
    } else if (year != null && month != null) {
      result = YearMonth.of(year.getValue(), month);
    } else if (year != null) {
      result = Year.of(year.getValue());
    }
    return Optional.ofNullable(result);
  }

  public static Optional<Integer> getStartDayOfYear(Temporal temporal) {
    return getDayOfYear(temporal, false);
  }

  public static Optional<Integer> getEndDayOfYear(Temporal temporal) {
    return getDayOfYear(temporal, true);
  }

  private static Optional<Integer> getDayOfYear(Temporal temporal, boolean isEndDay) {
    Integer result = null;
    if (temporal instanceof OffsetDateTime) {
      result = ((OffsetDateTime) temporal).getDayOfYear();
    } else if (temporal instanceof LocalDateTime) {
      result = ((LocalDateTime) temporal).getDayOfYear();
    } else if (temporal instanceof LocalDate) {
      result = ((LocalDate) temporal).getDayOfYear();
    } else if (temporal instanceof YearMonth) {
      YearMonth yearMonth = (YearMonth) temporal;
      if (isEndDay) {
        result = yearMonth.atEndOfMonth().getDayOfYear();
      } else {
        result = yearMonth.atDay(1).getDayOfYear();
      }
    } else if (temporal instanceof Year) {
      int year = ((Year) temporal).getValue();
      if (isEndDay) {
        result = YearMonth.of(year, 12).atEndOfMonth().getDayOfYear();
      } else {
        result = YearMonth.of(year, 1).atDay(1).getDayOfYear();
      }
    }
    return Optional.ofNullable(result);
  }
}
