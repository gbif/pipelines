package org.gbif.pipelines.core.parsers.temporal.parser;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.time.temporal.ChronoField;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

/**
 * Interpreter for raw date and time. The main method parse, fills year, month, day, hour, minute,
 * second and time zone
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParserRawDateTime {

  private static final Pattern RGX_YEAR = Pattern.compile("[0-9]{4}");
  private static final Pattern RGX_PATTERN = Pattern.compile("[0-9]{2}");

  /**
   * Parse year, month, day, hour, minute and second position in the raw date string, and save raw
   * values into ChronoAccumulator
   *
   * @param rawDate raw date and time string
   * @param lastParsed if it is date "to", it can store only one value
   * @return ChronoAccumulator which store all parsed values
   */
  public static ChronoAccumulator parse(String rawDate, ChronoField lastParsed) {
    if (isNullOrEmpty(rawDate)
        || (!RGX_YEAR.matcher(rawDate).find() && !RGX_PATTERN.matcher(rawDate).find())) {
      return new ChronoAccumulator();
    }

    String[] dateTimeArray = DelimiterUtils.splitDateTime(rawDate);

    // Interpret date and time separately
    ChronoAccumulator temporalDate = ParserRawDate.parse(dateTimeArray[0], lastParsed);
    ChronoAccumulator temporalTime = ParserRawTime.parse(dateTimeArray[1]);
    return temporalDate.mergeReplace(temporalTime);
  }
}
