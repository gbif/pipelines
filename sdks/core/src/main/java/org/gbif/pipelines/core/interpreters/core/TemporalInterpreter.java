package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssueSet;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValue;

import com.google.common.collect.Range;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.parsers.temporal.EventRange;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.TemporalRangeParser;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/** Interprets date representations into a Date to support API v1 */
@Slf4j
public class TemporalInterpreter implements Serializable {

  private static final long serialVersionUID = 410232939831224196L;

  private static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);
  private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1500, 1, 1);

  private final TemporalRangeParser temporalRangeParser;
  private final TemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private TemporalInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.preprocessDateFn = preprocessDateFn;
    this.temporalParser = TemporalParser.create(orderings);
    this.temporalRangeParser =
        TemporalRangeParser.builder().temporalParser(temporalParser).create();
  }

  public void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    String year = extractValue(er, DwcTerm.year);
    String month = extractValue(er, DwcTerm.month);
    String day = extractValue(er, DwcTerm.day);
    String eventDate = extractValue(er, DwcTerm.eventDate);

    String normalizedEventDate =
        Optional.ofNullable(preprocessDateFn).map(x -> x.apply(eventDate)).orElse(eventDate);

    EventRange eventRange = temporalRangeParser.parse(year, month, day, normalizedEventDate);

    Optional<TemporalAccessor> ta =
        eventRange.isReversed() ? eventRange.getTo() : eventRange.getFrom();

    ta.map(AtomizedLocalDate::fromTemporalAccessor)
        .ifPresent(
            ald -> {
              tr.setYear(ald.getYear());
              tr.setMonth(ald.getMonth());
              tr.setDay(ald.getDay());
            });

    EventDate ed = new EventDate();
    eventRange.getFrom().map(TemporalAccessor::toString).ifPresent(ed::setGte);
    eventRange.getTo().map(TemporalAccessor::toString).ifPresent(ed::setLte);
    tr.setEventDate(ed);

    addIssueSet(tr, eventRange.getIssues());
  }

  public void interpretModified(ExtendedRecord er, TemporalRecord tr) {
    if (hasValue(er, DcTerm.modified)) {
      String value = extractValue(er, DcTerm.modified);
      String normalizedValue =
          Optional.ofNullable(preprocessDateFn).map(x -> x.apply(value)).orElse(value);

      LocalDate upperBound = LocalDate.now().plusDays(1);
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseLocalDate(
              normalizedValue, validModifiedDateRange, OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
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
      String value = extractValue(er, DwcTerm.dateIdentified);
      String normalizedValue =
          Optional.ofNullable(preprocessDateFn).map(x -> x.apply(value)).orElse(value);

      LocalDate upperBound = LocalDate.now().plusDays(1);
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          temporalParser.parseLocalDate(
              normalizedValue, validRecordedDateRange, OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(parsed.getPayload())
            .map(TemporalAccessor::toString)
            .ifPresent(tr::setDateIdentified);
      }

      addIssueSet(tr, parsed.getIssues());
    }
  }

  /** Sets the coreId field. */
  public static void setCoreId(ExtendedRecord er, TemporalRecord tr) {
    Optional.ofNullable(er.getCoreId()).ifPresent(tr::setCoreId);
  }

  /** Sets the parentEventId field. */
  public static void setParentEventId(ExtendedRecord er, TemporalRecord tr) {
    extractOptValue(er, DwcTerm.parentEventID).ifPresent(tr::setParentId);
  }
}
