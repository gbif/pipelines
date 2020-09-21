package org.gbif.pipelines.core.parsers.temporal;

import com.google.common.base.Strings;
import java.text.ParseException;
import java.time.temporal.TemporalAccessor;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

public class TemporalRangeParser {
  private org.gbif.common.parsers.date.TemporalParser temporalParser;

  @Builder
  public TemporalRangeParser instanceOf(TemporalParser tp){
    TemporalRangeParser trp = new TemporalRangeParser();
    trp.temporalParser = tp;
    return trp;
  }

  /**
   * Try to parse a date string to a date range.
   *
   * @param dateRange  [startDate, endDate]
   * @return
   * @throws ParseException
   */
  public TemporalAccessor[] parse(String dateRange) throws ParseException {
    TemporalAccessor[] period = new TemporalAccessor[2];
    dateRange = normalizeDateString(dateRange);
    // Even a single date will be split to two
    String[] rawPeriod = DelimiterUtils.splitPeriod(dateRange);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];

    if (!Strings.isNullOrEmpty(rawFrom)) {
      ParseResult<TemporalAccessor> startParseResult = temporalParser.parse(rawPeriod[0]);
      if( startParseResult.isSuccessful()){
        period[0] = startParseResult.getPayload();
      }else{
        throw new ParseException(String.format("Start date in %s cannot be parsed!", dateRange), 0);
      }
    }

    if (!Strings.isNullOrEmpty(rawTo)) {
      ParseResult<TemporalAccessor> endParseResult = temporalParser.parse(rawPeriod[1]);
      if( endParseResult.isSuccessful()){
        period[0] = endParseResult.getPayload();
      }else{
        throw new ParseException(String.format("End date in %s cannot be parsed!", dateRange), 0);
      }
    }
    return period;
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
