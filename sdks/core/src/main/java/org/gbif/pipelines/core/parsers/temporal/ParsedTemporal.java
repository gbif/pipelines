package org.gbif.pipelines.core.parsers.temporal;

import java.time.LocalTime;
import java.time.Month;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.pipelines.core.parsers.temporal.utils.TemporalUtils;

/** Base temporal class, consists of two parsed dates from and to, also year, month and day */
@Getter
@Setter
@NoArgsConstructor(staticName = "create")
public class ParsedTemporal {

  private Year year;
  private Month month;
  private Integer day;
  private Temporal fromDate;
  private Temporal toDate;
  private Set<ParsedTemporalIssue> issues = Collections.emptySet();

  public static ParsedTemporal create(ParsedTemporalIssue issue) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setIssues(new HashSet<>(Collections.singleton(issue)));
    return parsedTemporal;
  }

  public static ParsedTemporal create(Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setIssues(issues);
    return parsedTemporal;
  }

  public static ParsedTemporal create(
      Temporal fromDate, Temporal toDate, Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setFromDate(fromDate);
    parsedTemporal.setToDate(toDate);
    parsedTemporal.setIssues(issues);
    return parsedTemporal;
  }

  public static ParsedTemporal create(
      Year year, Month month, Integer day, Temporal fromDate, Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setFromDate(fromDate);
    parsedTemporal.setYear(year);
    parsedTemporal.setMonth(month);
    parsedTemporal.setDay(day);
    parsedTemporal.setIssues(issues);
    return parsedTemporal;
  }

  public Optional<Temporal> getFromOpt() {
    return Optional.ofNullable(fromDate);
  }

  public Optional<Temporal> getToOpt() {
    return Optional.ofNullable(toDate);
  }

  public Optional<Year> getYearOpt() {
    return Optional.ofNullable(year);
  }

  public Optional<Month> getMonthOpt() {
    return Optional.ofNullable(month);
  }

  public Optional<Integer> getDayOpt() {
    return Optional.ofNullable(day);
  }

  public void setFromDate(Year year, Month month, Integer day, LocalTime time, ZoneOffset offset) {
    try {
      TemporalUtils.getTemporal(year, month, day, time, offset).ifPresent(x -> this.fromDate = x);
    } catch (RuntimeException ex) {
      issues.add(ParsedTemporalIssue.DATE_INVALID);
    }
  }

  public Optional<Integer> getStartDayOfYear() {
    return Optional.ofNullable(fromDate).flatMap(TemporalUtils::getStartDayOfYear);
  }

  public Optional<Integer> getEndDayOfYear() {
    Temporal temporal = Optional.ofNullable(toDate).orElse(fromDate);
    return Optional.ofNullable(temporal).flatMap(TemporalUtils::getEndDayOfYear);
  }
}
