package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.base.Strings;
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
import java.util.function.Supplier;
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
    return parse(null, null, null, dateRange);
  }

  public EventRange parse(String year, String month, String day, String dateRange) {
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(dateRange);

    EventRange eventRange = new EventRange();
    parseAndSetFrom(eventRange, year, month, day, rawPeriod[0], eventRange::setFrom);
    parseAndSetTo(eventRange, rawPeriod[1], eventRange::getFrom, eventRange::setTo, false);

    // FIX https://github.com/gbif/pipelines/issues/458
    if (rawPeriod.length == 2
        && !eventRange.getFrom().isPresent()
        && !eventRange.getTo().isPresent()
        && eventRange.hasIssues()) {
      EventRange reversed = new EventRange();
      parseAndSetFrom(reversed, year, month, day, rawPeriod[1], reversed::setTo);
      parseAndSetTo(reversed, rawPeriod[0], reversed::getTo, reversed::setFrom, true);
      if (reversed.getFrom().isPresent() && reversed.getTo().isPresent() && !reversed.hasIssues()) {
        reversed.setReversed(true);
        return reversed;
      }
    }

    return eventRange;
  }

  private void parseAndSetFrom(
      EventRange range,
      String year,
      String month,
      String day,
      String rawDate,
      Consumer<TemporalAccessor> setFromFn) {
    if (!Strings.isNullOrEmpty(rawDate) || !Strings.isNullOrEmpty(year)) {
      OccurrenceParseResult<TemporalAccessor> result =
          temporalParser.parseRecordedDate(year, month, day, rawDate);
      if (result.isSuccessful()) {
        Optional.ofNullable(result.getPayload()).ifPresent(setFromFn);
      }
      range.addIssues(result.getIssues());
    }
  }

  private void parseAndSetTo(
      EventRange range,
      String rawDate,
      Supplier<Optional<TemporalAccessor>> getFromFn,
      Consumer<TemporalAccessor> setToFn,
      boolean isReversed) {
    if (Strings.isNullOrEmpty(rawDate)) {
      return;
    }
    OccurrenceParseResult<TemporalAccessor> result = temporalParser.parseRecordedDate(rawDate);
    if (result.isSuccessful()) {
      Optional<TemporalAccessor> payload = Optional.ofNullable(result.getPayload());
      Optional<TemporalAccessor> fromOpt = getFromFn.get();
      if (isReversed) {
        payload = getFromFn.get();
        fromOpt = Optional.ofNullable(result.getPayload());
      }
      if (payload.isPresent() && fromOpt.isPresent()) {
        TemporalAccessor from = fromOpt.get();
        TemporalAccessor to = payload.get();
        if (from.getClass() == to.getClass()) {
          long rangeDiff = getRangeDiff((Temporal) from, (Temporal) to);
          if (rangeDiff > 0) {
            setToFn.accept(isReversed ? from : to);
          } else if (rangeDiff < 0) {
            range.addIssue(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
          }
        } else {
          range.addIssue(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
        }
      }
    }
    range.addIssues(result.getIssues());
  }

  /** Compare dates and returns difference between FROM and TO dates in milis */
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
