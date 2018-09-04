package org.gbif.pipelines.parsers.parsers.temporal;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Base temporal class, consists of two parsed dates from and to, also year, month and day */
public class ParsedTemporal {

  private Year year;
  private Month month;
  private Integer day;
  private Temporal fromDate;
  private Temporal toDate;
  private List<String> issueList = Collections.emptyList();

  public ParsedTemporal() {}

  public ParsedTemporal(Temporal fromDate, Temporal toDate) {
    this.fromDate = fromDate;
    this.toDate = toDate;
  }

  public ParsedTemporal(Year year, Month month, Integer day, Temporal fromDate) {
    this.year = year;
    this.month = month;
    this.day = day;
    this.fromDate = fromDate;
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

  public void setToDate(Temporal toDate) {
    this.toDate = toDate;
  }

  public List<String> getIssueList() {
    return issueList;
  }

  public void setIssueList(List<String> issueList) {
    this.issueList = issueList;
  }
}
