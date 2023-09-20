package org.gbif.pipelines.core.parsers.temporal;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import com.google.common.base.Strings;
import java.time.*;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.common.parsers.date.TemporalAccessorUtils;

/*
 * Note java.util.Date should be avoided, as it does not handle dates before 1582 without additional effort.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringToDateFunctions {

  // Supported Date formats
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern(
          "[yyyy[-MM[-dd['T'HH:mm[:ss[.SSSSSSSSS][.SSSSSSSS][.SSSSSSS][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]]]]][ ][XXXXX][XXXX][XXX][XX][X]]");

  // DateTimeFormatter.ISO_LOCAL_TIME but limited to milliseconds
  public static final DateTimeFormatter ISO_LOCAL_TIME_MILLISECONDS =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 3, true)
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // DateTimeFormatter.ISO_LOCAL_DATE_TIME but limited to milliseconds
  public static final DateTimeFormatter ISO_LOCAL_DATE_TIME_MILLISECONDS =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral('T')
          .append(ISO_LOCAL_TIME_MILLISECONDS)
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT)
          .withChronology(IsoChronology.INSTANCE);

  // DateTimeFormatter.ISO_OFFSET_DATE_TIME but limited to milliseconds
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_MILLISECONDS =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(ISO_LOCAL_DATE_TIME_MILLISECONDS)
          .appendOffsetId()
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT)
          .withChronology(IsoChronology.INSTANCE);

  public static Function<TemporalAccessor, String> getTemporalToStringFn() {
    return temporalAccessor -> {
      if (temporalAccessor instanceof ZonedDateTime) {
        return ((ZonedDateTime) temporalAccessor).format(ISO_OFFSET_DATE_TIME_MILLISECONDS);
      } else if (temporalAccessor instanceof LocalDateTime) {
        return ((LocalDateTime) temporalAccessor).format(ISO_LOCAL_DATE_TIME_MILLISECONDS);
      } else if (temporalAccessor instanceof LocalDate) {
        return ((LocalDate) temporalAccessor).format(DateTimeFormatter.ISO_LOCAL_DATE);
      } else {
        return temporalAccessor.toString();
      }
    };
  }

  public static Function<String, TemporalAccessor> getStringToTemporalAccessor() {
    return dateAsString -> {
      if (Strings.isNullOrEmpty(dateAsString)) {
        return null;
      }

      if (dateAsString.startsWith("0000")) {
        dateAsString = dateAsString.replaceFirst("0000", "1970");
      }

      try {
        // parse string
        return FORMATTER.parseBest(
            dateAsString,
            ZonedDateTime::from,
            LocalDateTime::from,
            LocalDate::from,
            YearMonth::from,
            Year::from);

      } catch (Exception ex) {
        return null;
      }
    };
  }

  public static Function<String, Long> getStringToEarliestEpochSeconds(boolean ignoreOffset) {
    return dateAsString -> {
      if (Strings.isNullOrEmpty(dateAsString)) {
        return null;
      }

      TemporalAccessor ta = getStringToTemporalAccessor().apply(dateAsString);
      return TemporalAccessorUtils.toEarliestLocalDateTime(ta, ignoreOffset)
          .toEpochSecond(ZoneOffset.UTC);
    };
  }

  public static Function<String, Long> getStringToLatestEpochSeconds(boolean ignoreOffset) {
    return dateAsString -> {
      if (Strings.isNullOrEmpty(dateAsString)) {
        return null;
      }

      TemporalAccessor ta = getStringToTemporalAccessor().apply(dateAsString);
      return TemporalAccessorUtils.toLatestLocalDateTime(ta, ignoreOffset)
          .toEpochSecond(ZoneOffset.UTC);
    };
  }
}
