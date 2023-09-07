package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.base.Strings;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringToDateFunctions {

  // Supported Date formats
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern(
          "[yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mmXXX][yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX]"
              + "[yyyy-MM-dd'T'HH:mm:ss.SSSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSS][yyyy-MM-dd'T'HH:mm:ss.SSS]"
              + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
              + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]");

  public static Function<TemporalAccessor, Date> getTemporalToDateFn() {
    return temporalAccessor -> {
      if (temporalAccessor instanceof ZonedDateTime) {
        return Date.from(((ZonedDateTime) temporalAccessor).toInstant());
      } else if (temporalAccessor instanceof LocalDateTime) {
        return Date.from(((LocalDateTime) temporalAccessor).toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof LocalDate) {
        return Date.from(((LocalDate) temporalAccessor).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof YearMonth) {
        return Date.from(
            ((YearMonth) temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof Year) {
        return Date.from(
            ((Year) temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else {
        return null;
      }
    };
  }

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

  public static Function<String, Date> getStringToDateFn() {
    return getStringToDateFn(false);
  }

  public static Function<String, Date> getStringToDateFn(boolean ignoreOffset) {
    return dateAsString -> {
      if (Strings.isNullOrEmpty(dateAsString)) {
        return null;
      }

      boolean firstYear = false;
      if (dateAsString.startsWith("0000")) {
        firstYear = true;
        dateAsString = dateAsString.replaceFirst("0000", "1970");
      }

      try {
        // parse string
        TemporalAccessor temporalAccessor =
            FORMATTER.parseBest(
                dateAsString,
                ZonedDateTime::from,
                LocalDateTime::from,
                LocalDate::from,
                YearMonth::from,
                Year::from);

        if (ignoreOffset && temporalAccessor instanceof ZonedDateTime) {
          temporalAccessor = ((ZonedDateTime) temporalAccessor).toLocalDateTime();
        }

        Date date = getTemporalToDateFn().apply(temporalAccessor);

        if (date != null && firstYear) {
          Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          cal.setTime(date);
          cal.set(Calendar.YEAR, 1);
          return cal.getTime();
        }

        return date;
      } catch (Exception ex) {
        return null;
      }
    };
  }
}
