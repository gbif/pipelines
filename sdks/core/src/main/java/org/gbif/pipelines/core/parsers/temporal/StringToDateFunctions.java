package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.base.Strings;
import java.time.*;
import java.time.format.DateTimeFormatter;
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
          "[yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mmXXX][yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX]"
              + "[yyyy-MM-dd'T'HH:mm:ss.SSSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSS][yyyy-MM-dd'T'HH:mm:ss.SSS]"
              + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
              + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]");

  public static Function<TemporalAccessor, String> getTemporalToStringFn() {
    return temporalAccessor -> {
      if (temporalAccessor instanceof ZonedDateTime) {
        return ((ZonedDateTime) temporalAccessor).format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
      } else if (temporalAccessor instanceof LocalDateTime) {
        return ((LocalDateTime) temporalAccessor).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
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
