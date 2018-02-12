package org.gbif.pipelines.interpretation.parsers.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

class ChronoAccumulator {

  private static final Map<ChronoField, Function<String, Integer>> FUNCTION_MAP = new EnumMap<>(ChronoField.class);
  private Map<ChronoField, Integer> valueMap = new EnumMap<>(ChronoField.class);
  private ChronoField lastParsed = null;

  static {
    FUNCTION_MAP.put(YEAR, ParsedDateUtils::parseYear);
    FUNCTION_MAP.put(MONTH_OF_YEAR, ParsedDateUtils::parseMonth);
    FUNCTION_MAP.put(DAY_OF_MONTH, ParsedDateUtils::parseDay);
    FUNCTION_MAP.put(HOUR_OF_DAY, ParsedDateUtils::parseHour);
    FUNCTION_MAP.put(MINUTE_OF_HOUR, ParsedDateUtils::parseMinute);
    FUNCTION_MAP.put(SECOND_OF_MINUTE, ParsedDateUtils::parseSecond);
  }

  static ChronoAccumulator from(String year, String month, String day) {
    ChronoAccumulator temporal = new ChronoAccumulator();
    temporal.convertAndPut(YEAR, year);
    temporal.convertAndPut(MONTH_OF_YEAR, month);
    temporal.convertAndPut(DAY_OF_MONTH, day);
    return temporal;
  }

  void convertAndPut(ChronoField key, String rawValue) {
    Integer value = FUNCTION_MAP.get(key).apply(rawValue);
    if (value != null) {
      valueMap.put(key, value);
      lastParsed = key;
    }
  }

  ChronoField getLastParsed() {
    return lastParsed;
  }

  ChronoAccumulator merge(ChronoAccumulator chronoAccumulator) {
    valueMap.putAll(chronoAccumulator.valueMap);
    if (chronoAccumulator.getLastParsed() != null) {
      lastParsed = chronoAccumulator.getLastParsed();
    }
    return this;
  }

  void putAll(ChronoAccumulator storage) {
    valueMap.putAll(storage.valueMap);
  }

  void putAllIfAbsent(ChronoAccumulator storage) {
    storage.valueMap.forEach(valueMap::putIfAbsent);
  }

  Temporal toTemporal() {
    Integer intYear = valueMap.get(YEAR);
    if (intYear == null) {
      return null;
    }
    Year year = Year.of(intYear);

    Integer intMonth = valueMap.get(MONTH_OF_YEAR);
    if (intMonth == null) {
      return year;
    }
    YearMonth yearMonth = year.atMonth(intMonth);

    Integer intDay = valueMap.get(DAY_OF_MONTH);
    if (intDay == null) {
      return yearMonth;
    }
    LocalDate localDate = yearMonth.atDay(intDay);

    Integer intHour = valueMap.get(HOUR_OF_DAY);
    if (intHour == null) {
      return localDate;
    }
    LocalDateTime localDateTime = localDate.atTime(intHour, 0);

    Integer intMonute = valueMap.get(MINUTE_OF_HOUR);
    if (intMonute == null) {
      return localDateTime;
    }
    localDateTime = localDateTime.withMinute(intMonute);

    Integer intSecond = valueMap.get(SECOND_OF_MINUTE);
    if (intSecond == null) {
      return localDateTime;
    }
    return localDateTime.withSecond(intSecond);
  }

}
