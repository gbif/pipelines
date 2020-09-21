package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.*;

import com.google.common.collect.Range;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/** Interprets date representations into a Date to support API v1 */
@Slf4j
public class TemporalInterpreter implements Serializable {

  private static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);
  private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);

  private final TemporalParser temporalParser;

  private TemporalInterpreter(DateComponentOrdering dateComponentOrdering) {
    this.temporalParser = TemporalParser.create(dateComponentOrdering);
  }

  public static TemporalInterpreter create(DateComponentOrdering dateComponentOrdering) {
    return new TemporalInterpreter(dateComponentOrdering);
  }

  public static TemporalInterpreter create() {
    return create(null);
  }

  public void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    String year = extractValue(er, DwcTerm.year);
    String month = extractValue(er, DwcTerm.month);
    String day = extractValue(er, DwcTerm.day);
    String eventDate = extractValue(er, DwcTerm.eventDate);

    OccurrenceParseResult<TemporalAccessor> eventResult =
        temporalParser.parseRecordedDate(year, month, day, eventDate);
    if (eventResult.isSuccessful()) {
      Optional<TemporalAccessor> temporalAccessor = Optional.ofNullable(eventResult.getPayload());

      Optional<TemporalAccessor> localDate =
          temporalAccessor
              .filter(ta -> ta.isSupported(ChronoField.HOUR_OF_DAY))
              .map(ta -> ta.query(TemporalQueries.localDate()));

      if (localDate.isPresent()) {
        temporalAccessor = localDate;
      }

      temporalAccessor
          .map(TemporalAccessor::toString)
          .ifPresent(x -> tr.setEventDate(new EventDate(x, null)));

      temporalAccessor
          .map(AtomizedLocalDate::fromTemporalAccessor)
          .ifPresent(
              ald -> {
                tr.setYear(ald.getYear());
                tr.setMonth(ald.getMonth());
                tr.setDay(ald.getDay());
              });
    }
    addIssueSet(tr, eventResult.getIssues());
  }

  public void interpretModified(ExtendedRecord er, TemporalRecord tr) {
    if (hasValue(er, DcTerm.modified)) {
      LocalDate upperBound = LocalDate.now().plusDays(1);
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseLocalDate(
              extractValue(er, DcTerm.modified),
              validModifiedDateRange,
              OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(TemporalAccessor::toString)
            .ifPresent(tr::setModified);
      }

      addIssueSet(tr, parsed.getIssues());
    }
  }

  public void interpretDateIdentified(ExtendedRecord er, TemporalRecord tr) {
    if (hasValue(er, DwcTerm.dateIdentified)) {
      LocalDate upperBound = LocalDate.now().plusDays(1);
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseLocalDate(
              extractValue(er, DwcTerm.dateIdentified),
              validRecordedDateRange,
              OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(TemporalAccessor::toString)
            .ifPresent(tr::setDateIdentified);
      }

      addIssueSet(tr, parsed.getIssues());
    }
  }
}
