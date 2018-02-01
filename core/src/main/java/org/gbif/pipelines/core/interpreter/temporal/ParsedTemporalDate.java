package org.gbif.pipelines.core.interpreter.temporal;

import java.time.Duration;

/**
 * Base temporal class, consists of two parsed dates from and to.
 */
public class ParsedTemporalDate {

  private ParsedDate fromDate = null;
  private ParsedDate toDate = null;
  private boolean hasIssue = true;

  public ParsedTemporalDate() {
  }

  public ParsedTemporalDate(ParsedDate fromDate) {
    this.fromDate = fromDate;
  }

  public ParsedTemporalDate(ParsedDate fromDate, ParsedDate toDate) {
    this.fromDate = fromDate;
    this.toDate = toDate;
    this.hasIssue = (fromDate != null && fromDate.hasIssue()) || (toDate != null && toDate.hasIssue());
  }

  public ParsedDate getFrom() {
    return fromDate;
  }

  public ParsedDate getTo() {
    return toDate;
  }

  public boolean hasIssue() {
    return hasIssue;
  }

  public Duration getDuration() {
    return fromDate == null || toDate == null ? Duration.ZERO : Duration.between(fromDate.toZonedDateTime(), toDate.toZonedDateTime());
  }

}
