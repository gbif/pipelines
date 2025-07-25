package org.gbif.pipelines.core.parsers.humboldt;

import java.util.Optional;
import lombok.Getter;
import org.elasticsearch.common.Strings;

public enum DurationUnit {
  seconds(0.0166667, "s", "sec", "secs"),
  minutes(1, "m", "min", "mins"),
  hours(60, "h", "hr", "hrs"),
  days(1440, "days", "d"),
  weeks(10080, "weeks", "wk", "wks"),
  months(43800, "months", "mo", "mos"),
  years(525600, "years", "yr", "yrs", "y");

  public static final DurationUnit DEFAULT = minutes;

  private double durationInMinutes;
  @Getter private final String[] alternativeNames;

  DurationUnit(double durationInMinutes, String... alternativeNames) {
    this.durationInMinutes = durationInMinutes;
    this.alternativeNames = alternativeNames;
  }

  public static Optional<DurationUnit> parseDurationUnit(String duration) {
    if (!Strings.isNullOrEmpty(duration)) {
      String normalizedDuration = duration.trim().toLowerCase();
      for (DurationUnit unit : DurationUnit.values()) {
        if (unit.name().startsWith(normalizedDuration)) {
          return Optional.of(unit);
        }

        for (String name : unit.getAlternativeNames()) {
          if (normalizedDuration.equals(name)) {
            return Optional.of(unit);
          }
        }
      }
    }
    return Optional.empty();
  }
}
