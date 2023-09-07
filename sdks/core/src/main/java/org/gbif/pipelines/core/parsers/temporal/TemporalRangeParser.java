package org.gbif.pipelines.core.parsers.temporal;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalUnit;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;

@Slf4j
public class TemporalRangeParser implements Serializable {

  private final TemporalParser temporalParser;

  @Builder(buildMethodName = "create")
  private TemporalRangeParser(TemporalParser temporalParser) {
    if (temporalParser != null) {
      this.temporalParser = temporalParser;
    } else {
      this.temporalParser = TemporalParser.create();
    }
  }

  public EventRange parse(String dateRange) {
    return parse(null, null, null, dateRange, null, null);
  }

  public EventRange parse(String year, String month, String day, String dateRange) {
    return parse(year, month, day, dateRange, null, null);
  }

  public EventRange parse(
      String year,
      String month,
      String day,
      String dateRange,
      String startDayOfYear,
      String endDayOfYear) {
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(dateRange);

    EventRange eventRange = new EventRange();
    // Year+month+day, first part of eventDate, and startDay of year to the best we can get.
    parseAndSet(eventRange, year, month, day, rawPeriod[0], startDayOfYear, eventRange::setFrom);
    // Year+month+day, second part of eventDate, and endDayOfYear of year to the best we can get.
    parseAndSet(eventRange, year, month, day, rawPeriod[1], endDayOfYear, eventRange::setTo);

    // Reverse order if needed
    if (eventRange.getFrom().isPresent() && eventRange.getTo().isPresent()) {
      TemporalAccessor from = eventRange.getFrom().get();
      TemporalAccessor to = eventRange.getTo().get();
      if (from.getClass() == to.getClass()) {
        long rangeDiff = getRangeDiff((Temporal) from, (Temporal) to);
        if (rangeDiff < 0) {
          eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_INVALID);
          EventRange reversed = new EventRange();
          reversed.setFrom(eventRange.getTo().get());
          reversed.setTo(eventRange.getFrom().get());
          reversed.setIssues(eventRange.getIssues());
          return reversed;
        }
      } else {
        eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
      }
    }

    return eventRange;
  }

  private void parseAndSet(
      EventRange range,
      String year,
      String month,
      String day,
      String rawDate,
      String dayOfYear,
      Consumer<TemporalAccessor> setFn) {
    OccurrenceParseResult<TemporalAccessor> result =
        temporalParser.parseRecordedDate(year, month, day, rawDate, dayOfYear);
    if (result.isSuccessful()) {
      Optional.ofNullable(result.getPayload()).ifPresent(setFn);
    }
    range.addIssues(result.getIssues());
  }

  /** Compare dates and returns difference between FROM and TO dates in milliseconds */
  private static long getRangeDiff(Temporal from, Temporal to) {
    if (from == null || to == null) {
      return 1L;
    }
    TemporalUnit unit = null;
    if (from instanceof Year) {
      unit = ChronoUnit.YEARS;
    } else if (from instanceof YearMonth) {
      unit = ChronoUnit.MONTHS;
    } else if (from instanceof LocalDate) {
      unit = ChronoUnit.DAYS;
    } else if (from instanceof LocalDateTime
        || from instanceof OffsetDateTime
        || from instanceof ZonedDateTime) {
      unit = ChronoUnit.SECONDS;
    }
    return from.until(to, unit);
  }
}
