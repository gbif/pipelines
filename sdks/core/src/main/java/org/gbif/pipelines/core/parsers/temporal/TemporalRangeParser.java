package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

@Slf4j
public class TemporalRangeParser {
  private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);
  private org.gbif.common.parsers.date.TemporalParser temporalParser;

  @Builder
  public static TemporalRangeParser instanceOf(TemporalParser temporalParser) {
    TemporalRangeParser trp = new TemporalRangeParser();
    trp.temporalParser = temporalParser;
    return trp;
  }

  /**
   * Try to parse a date string to a date range.
   *
   * @param dateRange [startDate, endDate]
   * @return
   * @throws ParseException
   */
  public TemporalAccessor[] parse(String dateRange) throws ParseException {
    // The last item stores the original start date - for precision check
    TemporalAccessor[] period = new TemporalAccessor[3];
    dateRange = normalizeDateString(dateRange);
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(dateRange);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];

    // If rawTo is empty/null, then it is not a OBVIOUS range.
    // Still need other checks after on dates like 2014, 2014-5
    boolean isRange = true;
    if (Strings.isNullOrEmpty(rawTo)) {
      isRange = false;
    }

    if (!Strings.isNullOrEmpty(rawFrom)) {
      ParseResult<TemporalAccessor> startParseResult = temporalParser.parse(rawPeriod[0]);
      if (startParseResult.isSuccessful()) {
        TemporalAccessor from = startParseResult.getPayload();
        if (isValidDate(from, true)) {
          period[2] = from; // Check original date precision
          if (isRange) {
            period[0] = TemporalAccessorUtils.toEarliestLocalDateTime(from, false);
          } else if (!from.isSupported(ChronoField.DAY_OF_MONTH)) {
            // Check on if data string is 2004, 2004-10 - they are also range
            period[0] = TemporalAccessorUtils.toEarliestLocalDateTime(from, false);
            period[1] = TemporalAccessorUtils.toLatestLocalDateTime(from, false);
          } else {
            period[0] = from;
          }
        } else {
          // Todo make sure assertion can be captured
          throw new ParseException("Unlikely", 0);
        }
      } else {
        log.error(String.format("Start date in %s cannot be parsed!", dateRange));
        throw new ParseException("Invalid", 1);
      }
    }

    if (isRange) {
      ParseResult<TemporalAccessor> endParseResult = temporalParser.parse(rawPeriod[1]);
      if (endParseResult.isSuccessful()) {
        TemporalAccessor to = endParseResult.getPayload();
        if (isValidDate(to, true)) {
          period[1] = TemporalAccessorUtils.toLatestLocalDateTime(to, false);
        } else {
          // Todo make sure assertion can be captured
          throw new ParseException("Unlikely", 0);
        }
      } else {
        log.error(String.format("End date in %s cannot be parsed!", dateRange));
        throw new ParseException("Invalid", 1);
      }
    }
    return period;
  }

  public TemporalAccessor[] parse(String year, String month, String day, String dateRange)
      throws ParseException {
    TemporalAccessor[] period = new TemporalAccessor[3];
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
      ParseResult<TemporalAccessor> startParseResult = temporalParser.parse(rawPeriod[0]);
      if (startParseResult.isSuccessful()) {
        TemporalAccessor from = startParseResult.getPayload();
        if (isValidDate(from, true)) {
          period[2] = from; // Check original date precision
          if (isRange) {
            period[0] = TemporalAccessorUtils.toEarliestLocalDateTime(from, false);
          } else if (!from.isSupported(ChronoField.DAY_OF_MONTH)) {
            // Check on if data string is 2004, 2004-10 - they are also range
            period[0] = TemporalAccessorUtils.toEarliestLocalDateTime(from, false);
            period[1] = TemporalAccessorUtils.toLatestLocalDateTime(from, false);
          } else {
            period[0] = from;
          }
        } else {
          // Todo make sure assertion can be captured
          throw new ParseException("Unlikely", 0);
        }
      } else if (startParseResult.getAlternativePayloads() != null) {
        // Try to solve ambigous
        period[0] = resolveConflicts(year, month, day, startParseResult);
      } else {
        log.error(String.format("Start date in %s cannot be parsed!", dateRange));
        throw new ParseException("Invalid", 1);
      }
    }

    if (isRange) {
      ParseResult<TemporalAccessor> endParseResult = temporalParser.parse(rawPeriod[1]);
      if (endParseResult.isSuccessful()) {
        TemporalAccessor to = endParseResult.getPayload();
        if (isValidDate(to, true)) {
          period[1] = TemporalAccessorUtils.toLatestLocalDateTime(to, false);
        } else {
          // Todo make sure assertion can be captured
          throw new ParseException("Unlikely", 0);
        }
      } else {
        log.error(String.format("End date in %s cannot be parsed!", dateRange));
        throw new ParseException("Invalid", 1);
      }
    }
    return period;
  }

  private TemporalAccessor resolveConflicts(
      String year, String month, String day, ParseResult<TemporalAccessor> src)
      throws ParseException {
    boolean ambiguityResolved = false;
    TemporalAccessor finalResult = src.getPayload();

    if (StringUtils.isNotBlank(year)
        || StringUtils.isNotBlank(month)
        || StringUtils.isNotBlank(day)) {
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
          log.debug("Date mismatch: [{} vs {}].", parsedYMDTa, src);
          throw new ParseException("Invalid", 1);
        }

        // choose the one with better resolution
        Optional<TemporalAccessor> bestResolution =
            TemporalAccessorUtils.bestResolution(parsedYMDTa, src.getPayload());
        if (bestResolution.isPresent()) {
          finalResult = bestResolution.get();
        }
      }
    }
    return finalResult;
  }

  @VisibleForTesting
  protected static boolean isValidDate(
      TemporalAccessor temporalAccessor, boolean acceptPartialDate) {
    LocalDate upperBound = LocalDate.now().plusDays(1);
    return isValidDate(
        temporalAccessor, acceptPartialDate, Range.closed(MIN_LOCAL_DATE, upperBound));
  }

  /** Check if a date express as TemporalAccessor falls between the provided range. */
  private static boolean isValidDate(
      TemporalAccessor temporalAccessor, boolean acceptPartialDate, Range<LocalDate> likelyRange) {

    if (temporalAccessor == null) {
      return false;
    }

    if (!acceptPartialDate) {
      LocalDate localDate = temporalAccessor.query(TemporalQueries.localDate());
      if (localDate == null) {
        return false;
      }
      return likelyRange.contains(localDate);
    }

    // if partial dates should be considered valid
    int year;
    int month = 1;
    int day = 1;
    if (temporalAccessor.isSupported(ChronoField.YEAR)) {
      year = temporalAccessor.get(ChronoField.YEAR);
    } else {
      return false;
    }

    if (temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)) {
      month = temporalAccessor.get(ChronoField.MONTH_OF_YEAR);
    }

    if (temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)) {
      day = temporalAccessor.get(ChronoField.DAY_OF_MONTH);
    }

    return likelyRange.contains(LocalDate.of(year, month, day));
  }

  /**
   * Preprocess for converting some none ISO standards to ISO standards
   *
   * @param dateString
   * @return
   */
  private String normalizeDateString(String dateString) {
    // Convert 2004-2-1 to 3-2 , 2004-2-1 & 3-2  to 2004-2-1/3-2
    if (StringUtils.isNotEmpty(dateString)) {
      dateString = dateString.replace(" to ", "/");
      dateString = dateString.replace(" & ", "/");
    }
    return dateString;
  }
}
