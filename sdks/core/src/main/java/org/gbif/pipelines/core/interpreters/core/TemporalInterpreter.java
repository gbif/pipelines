package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssueSet;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValue;

import com.google.common.collect.Range;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.EventRange;
import org.gbif.common.parsers.date.MultiinputTemporalParser;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalRangeParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/** Interprets date representations into Dates and EventDate (ranges) to support API v1 */
@Slf4j
public class TemporalInterpreter implements Serializable {

  private static final long serialVersionUID = 410232939831224196L;

  private static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);

  // The start of Linnean taxonomy. See https://github.com/tdwg/bdq/issues/76.
  private static final LocalDate EARLIEST_DATE_IDENTIFIED = LocalDate.of(1753, 1, 1);

  private final TemporalRangeParser temporalRangeParser;
  private final MultiinputTemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;
  private final boolean explicitRangeEnd;

  @Builder(buildMethodName = "create")
  private TemporalInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn,
      Boolean explicitRangeEnd) {
    this.preprocessDateFn = preprocessDateFn;
    this.temporalParser = MultiinputTemporalParser.create(orderings);
    this.temporalRangeParser =
        TemporalRangeParser.builder().temporalParser(temporalParser).create();
    this.explicitRangeEnd = explicitRangeEnd == null ? true : false;
  }

  public void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    String year = extractValue(er, DwcTerm.year);
    String month = extractValue(er, DwcTerm.month);
    String day = extractValue(er, DwcTerm.day);
    String startDayOfYear = extractValue(er, DwcTerm.startDayOfYear);
    String endDayOfYear = extractValue(er, DwcTerm.endDayOfYear);
    String eventDate = extractValue(er, DwcTerm.eventDate);

    String normalizedEventDate =
        Optional.ofNullable(preprocessDateFn).map(x -> x.apply(eventDate)).orElse(eventDate);

    EventRange eventRange =
        temporalRangeParser.parse(
            year, month, day, normalizedEventDate, startDayOfYear, endDayOfYear);

    Optional<TemporalAccessor> fromTa = eventRange.getFrom();
    Optional<TemporalAccessor> toTa = eventRange.getTo();

    Optional<AtomizedLocalDate> fromYmd =
        eventRange.getFrom().map(AtomizedLocalDate::fromTemporalAccessor);
    Optional<AtomizedLocalDate> toYmd =
        eventRange.getTo().map(AtomizedLocalDate::fromTemporalAccessor);

    if (fromYmd.isPresent() && toYmd.isPresent()) {
      if (Objects.equals(fromYmd.get().getYear(), toYmd.get().getYear())) {
        tr.setYear(fromYmd.get().getYear());
        if (Objects.equals(fromYmd.get().getMonth(), toYmd.get().getMonth())) {
          tr.setMonth(fromYmd.get().getMonth());
          if (Objects.equals(fromYmd.get().getDay(), toYmd.get().getDay())) {
            tr.setDay(fromYmd.get().getDay());
          }
        }
      }
    }

    fromTa
        .filter(t -> t.isSupported(ChronoField.DAY_OF_YEAR))
        .ifPresent(t -> tr.setStartDayOfYear(t.get(ChronoField.DAY_OF_YEAR)));
    toTa.filter(t -> t.isSupported(ChronoField.DAY_OF_YEAR))
        .ifPresent(t -> tr.setEndDayOfYear(t.get(ChronoField.DAY_OF_YEAR)));

    EventDate ed = new EventDate();
    eventRange
        .getFrom()
        .map(ta -> TemporalAccessorUtils.stripOffsetOrZone(ta, true))
        .map(StringToDateFunctions.getTemporalToStringFn())
        .ifPresent(ed::setGte);
    if (explicitRangeEnd || (fromTa != null && !fromTa.equals(toTa))) {
      eventRange
          .getTo()
          .map(ta -> TemporalAccessorUtils.stripOffsetOrZone(ta, true))
          .map(StringToDateFunctions.getTemporalToStringFn())
          .ifPresent(ed::setLte);
    }
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
      Range<LocalDate> validRecordedDateRange = Range.closed(EARLIEST_DATE_IDENTIFIED, upperBound);
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
