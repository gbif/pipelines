package org.gbif.pipelines.core.interpreter.temporal;

/**
 * Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class RawTemporalInterpreter {

  private RawTemporalInterpreter() {
    //NOP
  }

  private static final String RGX_PERIOD = "/";

  public static ParsedTemporalDate interpret(ParsedDate baseParsedDate, String rDate){
    ParsedDate baseDate = ParsedDate.copy(baseParsedDate);

    String[] periodRawDates = splitByPeriod(rDate);

    ParsedDate fromDate = RawDateTimeInterpreter.interpret(baseDate, periodRawDates[0]);
    ParsedDate toDate = RawDateTimeInterpreter.interpret(fromDate, periodRawDates[1]);

    return new ParsedTemporalDate(fromDate, toDate);
  }


  /**
   * Attempt to split the rDate into raw periods by '/' symbol, the symbol must be the only one in the rDate
   *
   * @param rDate raw string date
   * @return always two elements array
   */
  private static String[] splitByPeriod(String rDate) {
    //If the spliterator for the first position and last position are the same, symbol only one in string,
    //check length, the length must be greater than seven to avoid case as "1999/2", where it looks as year and month
    boolean canSplit = rDate.lastIndexOf(RGX_PERIOD) == rDate.indexOf(RGX_PERIOD) && rDate.length() > 7;
    String[] splited = canSplit ? rDate.split(RGX_PERIOD) : new String[] {rDate, ""};
    //Returns an array of the same length each time
    return splited.length < 2 ? new String[] {splited[0], ""} : splited;
  }
}
