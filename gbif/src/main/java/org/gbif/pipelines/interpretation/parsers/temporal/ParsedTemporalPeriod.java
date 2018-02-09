package org.gbif.pipelines.interpretation.parsers.temporal;

import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.Optional;

/**
 * Base temporal class, consists of two parsed dates from and to.
 */
public class ParsedTemporalPeriod {

  private Temporal fromDate = null;
  private Temporal toDate = null;

  public ParsedTemporalPeriod() {
  }

  public ParsedTemporalPeriod(Temporal fromDate) {
    this.fromDate = fromDate;
  }

  public ParsedTemporalPeriod(Temporal fromDate, Temporal toDate) {
    this.fromDate = fromDate;
    this.toDate = toDate;
  }

  public Optional<Temporal> getFrom() {
    return Optional.ofNullable(fromDate);
  }

  public Optional<Temporal> getTo() {
    return Optional.ofNullable(toDate);
  }

  public Duration getDuration() {
    return fromDate == null || toDate == null ? Duration.ZERO : Duration.between(fromDate, toDate);
  }

  public String toEsString() {
    String from = fromDate != null ? fromDate.toString() : "";
    String to = toDate != null ? toDate.toString() : "";
    String duration = String.valueOf(getDuration().getSeconds());
    return "".equals(to) ? from : String.join("||", from, to, duration);
  }

}
