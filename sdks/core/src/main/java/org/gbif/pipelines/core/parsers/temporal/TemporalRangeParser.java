package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.base.Strings;
import java.io.Serializable;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.core.OccurrenceParseResult;

@Slf4j
public class TemporalRangeParser implements Serializable {

  private final TemporalParser temporalParser;
  @Builder.Default private Map<String, String> normalizeMap = Collections.emptyMap();

  @Builder(buildMethodName = "create")
  private TemporalRangeParser(TemporalParser temporalParser, Map<String, String> normalizeMap) {
    this.normalizeMap = normalizeMap;
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
    String normalizedString = normalizeDateString(dateRange);
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(normalizedString);

    EventRange eventRange = new EventRange();
    parseAndSetFrom(eventRange, year, month, day, rawPeriod[0]);
    parseAndSetTo(eventRange, rawPeriod[1]);

    return eventRange;
  }

  private void parseAndSetFrom(
      EventRange range, String year, String month, String day, String rawDate) {
    if (!Strings.isNullOrEmpty(rawDate)) {
      OccurrenceParseResult<TemporalAccessor> result =
          temporalParser.parseRecordedDate(year, month, day, rawDate);
      if (result.isSuccessful()) {
        Optional.ofNullable(result.getPayload()).ifPresent(range::setFrom);
      }
      range.addIssues(result.getIssues());
    }
  }

  private void parseAndSetTo(EventRange range, String rawDate) {
    if (!Strings.isNullOrEmpty(rawDate)) {
      OccurrenceParseResult<TemporalAccessor> result = temporalParser.parseRecordedDate(rawDate);
      if (result.isSuccessful()) {
        Optional<TemporalAccessor> payload = Optional.ofNullable(result.getPayload());
        if (payload.isPresent() && range.getFrom().isPresent()) {
          TemporalAccessor from = range.getFrom().get();
          TemporalAccessor to = payload.get();
          if (from.getClass() == to.getClass() && isValidRange((Temporal) from, (Temporal) to)) {
            range.setTo(to);
          }
        }
      }
      range.addIssues(result.getIssues());
    }
  }

  /** Compare dates, FROM cannot be greater than TO */
  private static boolean isValidRange(Temporal from, Temporal to) {
    if (from == null || to == null) {
      return true;
    }
    TemporalUnit unit = null;
    if (from instanceof Year) {
      unit = ChronoUnit.YEARS;
    } else if (from instanceof YearMonth) {
      unit = ChronoUnit.MONTHS;
    } else if (from instanceof LocalDate) {
      unit = ChronoUnit.DAYS;
    } else if (from instanceof LocalDateTime || from instanceof OffsetDateTime) {
      unit = ChronoUnit.SECONDS;
    }
    return from.until(to, unit) > 0;
  }

  /** Preprocess for converting some none ISO standards to ISO standards */
  private String normalizeDateString(String dateString) {
    // Convert 2004-2-1 to 3-2 , 2004-2-1 & 3-2  to 2004-2-1/3-2
    if (StringUtils.isNotEmpty(dateString) && normalizeMap != null) {
      for (Map.Entry<String, String> en : normalizeMap.entrySet()) {
        dateString = dateString.replace(en.getKey(), en.getValue());
      }
    }
    return dateString;
  }
}
