package org.gbif.pipelines.core.parsers.temporal.accumulator;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.time.temporal.ChronoField;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/** The accumulator class for storing all parsed chrono fields */
public class ChronoAccumulator {

  private final Map<ChronoField, String> valueMap = new EnumMap<>(ChronoField.class);

  private ChronoField lastParsed;

  public static ChronoAccumulator from(String year, String month, String day) {
    ChronoAccumulator temporal = new ChronoAccumulator();
    temporal.setChronoField(YEAR, year);
    temporal.setChronoField(MONTH_OF_YEAR, month);
    temporal.setChronoField(DAY_OF_MONTH, day);
    return temporal;
  }

  /**
   * Set raw value
   *
   * @param chronoField one of the ChronoFields: YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY,
   *     MINUTE_OF_HOUR, SECOND_OF_MINUTE
   * @param rawValue raw value for parsing
   */
  public void setChronoField(ChronoField chronoField, String rawValue) {
    if (!isEmpty(rawValue)) {
      valueMap.put(chronoField, rawValue);
      lastParsed = chronoField;
    }
  }

  /**
   * Get raw value by ChronoField
   *
   * @return raw string value
   */
  public Optional<String> getChronoFileValue(ChronoField chronoField) {
    return Optional.ofNullable(valueMap.get(chronoField));
  }

  /** @return last parsed chrono field */
  public Optional<ChronoField> getLastParsed() {
    return Optional.ofNullable(lastParsed);
  }

  /**
   * Copies ALL of the values from the specified accumulator to this accumulator. If a chrono filed
   * is present, the field will be replaced by the value from accumulator param
   */
  public ChronoAccumulator mergeReplace(ChronoAccumulator accumulator) {
    valueMap.putAll(accumulator.valueMap);
    accumulator.getLastParsed().ifPresent(chronoField -> lastParsed = chronoField);
    return this;
  }

  /**
   * Copies CHRONO FILED values from the specified accumulator to this accumulator. If a chrono
   * filed is present, the field will NOT be replaced by the value from accumulator param
   */
  public ChronoAccumulator mergeAbsent(ChronoAccumulator accumulator) {
    accumulator.valueMap.forEach(valueMap::putIfAbsent);
    return this;
  }
}
