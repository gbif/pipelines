package org.gbif.pipelines.parsers.parsers.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NoArgsConstructor;

/** Base temporal class, consists of two parsed dates from and to, also year, month and day */
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
    parsedTemporal.setIssueSet(new HashSet<>(Collections.singleton(issue)));
    return parsedTemporal;
  }

  public static ParsedTemporal create(Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setIssueSet(issues);
    return parsedTemporal;
  }

  public static ParsedTemporal create(Temporal fromDate, Temporal toDate, Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setFromDate(fromDate);
    parsedTemporal.setToDate(toDate);
    parsedTemporal.setIssueSet(issues);
    return parsedTemporal;
  }

  public static ParsedTemporal create(Year year, Month month, Integer day, Temporal fromDate,
      Set<ParsedTemporalIssue> issues) {
    ParsedTemporal parsedTemporal = create();
    parsedTemporal.setFromDate(fromDate);
    parsedTemporal.setYear(year);
    parsedTemporal.setMonth(month);
    parsedTemporal.setDay(day);
    parsedTemporal.setIssueSet(issues);
    return parsedTemporal;
  }

  public Optional<Temporal> getFrom() {
    return Optional.ofNullable(fromDate);
  }

  public Optional<Temporal> getTo() {
    return Optional.ofNullable(toDate);
  }

  public Optional<Year> getYear() {
    return Optional.ofNullable(year);
  }

  public Optional<Month> getMonth() {
    return Optional.ofNullable(month);
  }

  public Optional<Integer> getDay() {
    return Optional.ofNullable(day);
  }

  public void setYear(Year year) {
    this.year = year;
  }

  public void setMonth(Month month) {
    this.month = month;
  }

  public void setDay(Integer day) {
    this.day = day;
  }

  public void setFromDate(Temporal fromDate) {
    this.fromDate = fromDate;
  }

  public void setFromDate(Year year, Month month, Integer day, LocalTime time) {
    if (year != null && month != null && day != null && time != null) {
      this.fromDate = LocalDateTime.of(LocalDate.of(year.getValue(), month, day), time);
    } else if (year != null && month != null && day != null) {
      this.fromDate = LocalDate.of(year.getValue(), month, day);
    } else if (year != null && month != null) {
      this.fromDate = YearMonth.of(year.getValue(), month);
    } else if (year != null) {
      this.fromDate = Year.of(year.getValue());
    }
  }

  public void setToDate(Temporal toDate) {
    this.toDate = toDate;
  }

  public Set<ParsedTemporalIssue> getIssueSet() {
    return issues;
  }

  public void setIssueSet(Set<ParsedTemporalIssue> issues) {
    this.issues = issues;
  }

  public List<String> getIssueList() {
    return issues.stream().map(ParsedTemporalIssue::name).collect(Collectors.toList());
  }
}
