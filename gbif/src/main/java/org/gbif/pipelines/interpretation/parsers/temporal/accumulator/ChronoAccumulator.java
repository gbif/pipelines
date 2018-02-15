package org.gbif.pipelines.interpretation.parsers.temporal.accumulator;

import java.time.temporal.ChronoField;
import java.util.EnumMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * The accumulator class for storing all parsed chrono fields
 */
public class ChronoAccumulator {

  Map<ChronoField, String> valueMap = new EnumMap<>(ChronoField.class);
  private ChronoField lastParsed = null;

  public static ChronoAccumulator from(String year, String month, String day) {
    ChronoAccumulator temporal = new ChronoAccumulator();
    temporal.put(YEAR, year);
    temporal.put(MONTH_OF_YEAR, month);
    temporal.put(DAY_OF_MONTH, day);
    return temporal;
  }

  /**
   * Converts raw value to integer and put into the map
   *
   * @param key      one of the ChronoFields: YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE
   * @param rawValue raw value for parsing
   */
  public void put(ChronoField key, String rawValue) {
    if (StringUtils.isEmpty(rawValue)) {
      return;
    }
    valueMap.put(key, rawValue);
    lastParsed = key;
  }

  /**
   * @return last parsed chrono field
   */
  public ChronoField getLastParsed() {
    return lastParsed;
  }

  /**
   * Copies ALL of the values from the specified accumulator to this accumulator.
   * If a chrono filed is present, the field will be replaced by the value from accumulator param
   */
  public ChronoAccumulator merge(ChronoAccumulator chronoAccumulator) {
    valueMap.putAll(chronoAccumulator.valueMap);
    if (chronoAccumulator.getLastParsed() != null) {
      lastParsed = chronoAccumulator.getLastParsed();
    }
    return this;
  }

  /**
   * Copies CHRONO FILED values from the specified accumulator to this accumulator.
   * If a chrono filed is present, the field will be replaced by the value from accumulator param
   */
  public void putAllAndReplce(ChronoAccumulator accumulator) {
    valueMap.putAll(accumulator.valueMap);
  }

  /**
   * Copies CHRONO FILED values from the specified accumulator to this accumulator.
   * If a chrono filed is present, the field will NOT be replaced by the value from accumulator param
   */
  public void putAllIfAbsent(ChronoAccumulator accumulator) {
    accumulator.valueMap.forEach(valueMap::putIfAbsent);
  }

  /**
   * Checks all value in the folder are numeric, except month
   */
  public boolean areAllNumeric() {
    return valueMap.entrySet().stream().anyMatch(x -> !x.getKey().equals(MONTH_OF_YEAR) && !isNumeric(x.getValue()));
  }

}
