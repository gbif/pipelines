package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.*;
import static org.gbif.pipelines.core.parsers.temporal.TemporalParser.isValidDate;

import com.google.common.base.Strings;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.CustomizedTextDateParser;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;

@Slf4j
public class TemporalRangeParser {

  private final org.gbif.common.parsers.date.TemporalParser temporalParser;
  @Builder.Default private Map<String, String> normalizeMap = Collections.emptyMap();

  @Builder(buildMethodName = "create")
  private TemporalRangeParser(
      DateComponentOrdering dateComponentOrdering, Map<String, String> normalizeMap) {
    this.normalizeMap = normalizeMap;
    if (dateComponentOrdering != null) {

      DateComponentOrdering[] orderings;
      if (dateComponentOrdering.name().equalsIgnoreCase("DMY")) {
        orderings = DMY_FORMATS;
      } else if (dateComponentOrdering.name().equalsIgnoreCase("MDY")) {
        orderings = MDY_FORMATS;
      } else {
        orderings = ISO_FORMATS;
      }

      temporalParser = CustomizedTextDateParser.getInstance(orderings);
    } else {
      temporalParser = DateParsers.defaultTemporalParser();
    }
  }

  /** Try to parse a date string to a date range. */
  public EventRange parse(String dateRange) {
    return parse(null, null, null, dateRange);
  }

  public EventRange parse(String year, String month, String day, String dateRange) {
    EventRange eventRange = new EventRange();
    dateRange = normalizeDateString(dateRange);
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(dateRange);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];

    // If rawTo is empty/null, then it is not a range.
    // If it is not a range, then start date does not need to calculate to the earliest datetime.
    boolean isRange = true;
    if (Strings.isNullOrEmpty(rawTo)) {
      isRange = false;
    }

    if (!Strings.isNullOrEmpty(rawFrom)) {
      ParseResult<TemporalAccessor> startParseResult = temporalParser.parse(rawFrom);
      if (startParseResult.isSuccessful()) {
        TemporalAccessor from = startParseResult.getPayload();
        if (isValidDate(from)) {
          eventRange.setOriginal(from); // Check original date precision
          if (isRange) {
            eventRange.setFrom(TemporalAccessorUtils.toEarliestLocalDateTime(from, false));
          } else if (!from.isSupported(ChronoField.DAY_OF_MONTH)) {
            // Check on if data string is 2004, 2004-10 - they are also range
            eventRange.setFrom(TemporalAccessorUtils.toEarliestLocalDateTime(from, false));
            eventRange.setTo(TemporalAccessorUtils.toLatestLocalDateTime(from, false));
          } else {
            eventRange.setFrom(from);
          }
        } else {
          eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
          return eventRange;
        }
      } else if (startParseResult.getAlternativePayloads() != null) {
        // Try to solve ambigous
        Optional<TemporalAccessor> optional = resolveConflicts(year, month, day, startParseResult);
        if (optional.isPresent()) {
          eventRange.setFrom(optional.get());
        } else {
          eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_INVALID);
          return eventRange;
        }
      } else {
        eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_INVALID);
        return eventRange;
      }
    }

    if (isRange) {
      ParseResult<TemporalAccessor> endParseResult = temporalParser.parse(rawTo);
      if (endParseResult.isSuccessful()) {
        TemporalAccessor to = endParseResult.getPayload();
        if (isValidDate(to)) {
          eventRange.setTo(TemporalAccessorUtils.toLatestLocalDateTime(to, false));
        } else {
          eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
          return eventRange;
        }
      } else {
        eventRange.addIssue(OccurrenceIssue.RECORDED_DATE_INVALID);
        return eventRange;
      }
    }
    return eventRange;
  }

  private Optional<TemporalAccessor> resolveConflicts(
      String year, String month, String day, ParseResult<TemporalAccessor> src) {
    boolean ambiguityResolved = false;
    TemporalAccessor finalResult = src.getPayload();

    if (!StringUtils.isNotBlank(year)
        && !StringUtils.isNotBlank(month)
        && !StringUtils.isNotBlank(day)) {
      return Optional.ofNullable(finalResult);
    }

    ParseResult<TemporalAccessor> parsedYMDResult = temporalParser.parse(year, month, day);
    TemporalAccessor parsedYMDTa = parsedYMDResult.getPayload();
    if (parsedYMDResult.isSuccessful()
        && !TemporalAccessorUtils.sameOrContained(parsedYMDTa, src.getPayload())) {
      // eventDate could be ambiguous (5/4/2014), but disambiguated by year-month-day.
      if (src.getAlternativePayloads() != null) {
        for (TemporalAccessor possibleTa : src.getAlternativePayloads()) {
          if (TemporalAccessorUtils.sameOrContained(parsedYMDTa, possibleTa)) {
            finalResult = possibleTa;
            ambiguityResolved = true;
          }
        }
      }

      // still a conflict
      if (!ambiguityResolved) {
        return Optional.empty();
      }

      // choose the one with better resolution
      Optional<TemporalAccessor> bestResolution =
          TemporalAccessorUtils.bestResolution(parsedYMDTa, src.getPayload());
      if (bestResolution.isPresent()) {
        finalResult = bestResolution.get();
      }
    }
    return Optional.ofNullable(finalResult);
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
