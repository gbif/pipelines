package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.*;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * Extensions to {@link org.gbif.pipelines.core.interpreters.core.TemporalInterpreter} for living
 * atlases.
 */
public class ALATemporalInterpreter implements Serializable {

  public static final String DAY_RANGE_PRECISION = "DAY_RANGE";
  public static final String MONTH_RANGE_PRECISION = "MONTH_RANGE";
  public static final String YEAR_RANGE_PRECISION = "YEAR_RANGE";
  public static final String NOT_SUPPLIED = "NOT_SUPPLIED";
  public static final String DAY_PRECISION = "DAY";
  public static final String MONTH_PRECISION = "MONTH";
  public static final String YEAR_PRECISION = "YEAR";
  protected static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);

  private final TemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private ALATemporalInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.preprocessDateFn = preprocessDateFn;
    this.temporalParser = TemporalParser.create(orderings);
  }

  /**
   * Code copied from GBIF. Add an extra assertion
   *
   * <p>Raise Missing_COLLECTION_DATE ASSERTION
   */
  public void checkRecordDateQuality(ExtendedRecord er, TemporalRecord tr) {
    final String year = extractValue(er, DwcTerm.year);
    final String month = extractValue(er, DwcTerm.month);
    final String day = extractValue(er, DwcTerm.day);
    final String dateString = extractValue(er, DwcTerm.eventDate);
    boolean atomizedDateProvided =
        StringUtils.isNotBlank(year)
            || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);

    if (!atomizedDateProvided && !dateStringProvided) {
      addIssue(tr, ALAOccurrenceIssue.MISSING_COLLECTION_DATE.name());
    }

    if (tr.getDay() != null && tr.getDay() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_MONTH.name());
    }
    if (tr.getMonth() != null && tr.getMonth() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_YEAR.name());
    }
    if (tr.getYear() != null && tr.getYear() % 100 == 0) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_CENTURY.name());
    }
  }

  /** All verification process require TemporalInterpreter.interpretTemporal has been called. */
  public void checkDateIdentified(ExtendedRecord er, TemporalRecord tr) {
    if (tr.getEventDate() != null && tr.getDateIdentified() != null) {

      ParseResult<TemporalAccessor> parsedIdentifiedResult =
          temporalParser.parseRecordedDate(tr.getDateIdentified());
      ParseResult<TemporalAccessor> parsedEventDateResult =
          temporalParser.parseRecordedDate(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful()
          && parsedIdentifiedResult.isSuccessful()
          && TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
              .after(TemporalAccessorUtils.toDate(parsedIdentifiedResult.getPayload()))) {
        addIssue(tr, ALAOccurrenceIssue.ID_PRE_OCCURRENCE.name());
      }
    }
  }

  /** All verification process require TemporalInterpreter.interpretTemporal has been called. */
  public void checkGeoreferencedDate(ExtendedRecord er, TemporalRecord tr) {
    if (tr.getEventDate() != null && hasValue(er, DwcTerm.georeferencedDate)) {
      ParseResult<TemporalAccessor> parsedGeoreferencedResult =
          temporalParser.parseRecordedDate(extractValue(er, DwcTerm.georeferencedDate));
      ParseResult<TemporalAccessor> parsedEventDateResult =
          temporalParser.parseRecordedDate(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful()
          && parsedGeoreferencedResult.isSuccessful()
          && TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
              .before(TemporalAccessorUtils.toDate(parsedGeoreferencedResult.getPayload()))) {
        addIssue(tr, ALAOccurrenceIssue.GEOREFERENCE_POST_OCCURRENCE.name());
      }
    }
  }

  static final int YEAR_PRECISION_LENGTH = "yyyy".length();
  static final int MONTH_PRECISION_LENGTH = "yyyy-MM".length();
  static final int DAY_PRECISION_LENGTH = "yyyy-MM-dd".length();

  /** All verification process require TemporalInterpreter.interpretTemporal has been called. */
  public static void checkDatePrecision(ExtendedRecord er, TemporalRecord tr) {

    String determinedDatePrecision = NOT_SUPPLIED;

    if (tr.getEventDate() != null) {

      // do we have a range
      if (tr.getEventDate().getLte() != null && tr.getEventDate().getGte() != null) {

        determinedDatePrecision =
            DAY_RANGE_PRECISION; // assume day range precision, then downgrade as required

        // catch
        if (tr.getEventDate().getLte().equals(tr.getEventDate().getGte())) {
          if (tr.getEventDate().getLte().length() == DAY_PRECISION_LENGTH) {
            determinedDatePrecision = DAY_PRECISION;
          }
          if (tr.getEventDate().getLte().length() == MONTH_PRECISION_LENGTH) {
            determinedDatePrecision = MONTH_PRECISION;
          }
          if (tr.getEventDate().getLte().length() == YEAR_PRECISION_LENGTH) {
            determinedDatePrecision = YEAR_PRECISION;
          }
        }

        String startDay = null;
        String startMonth = null;
        String startYear = null;

        if (tr.getEventDate().getGte().length() >= YEAR_PRECISION_LENGTH) {
          // first 4 chars
          startYear = tr.getEventDate().getGte().substring(0, 4);
        }
        if (tr.getEventDate().getGte().length() >= MONTH_PRECISION_LENGTH) {
          // first 4 chars
          startMonth = tr.getEventDate().getGte().substring(5, 7);
        }
        if (tr.getEventDate().getGte().length() == DAY_PRECISION_LENGTH) {
          // first 4 chars
          startDay = tr.getEventDate().getGte().substring(8, 10);
        }

        String endDay = null;
        String endMonth = null;
        String endYear = null;

        if (tr.getEventDate().getLte().length() >= YEAR_PRECISION_LENGTH) {
          // first 4 chars
          endYear = tr.getEventDate().getLte().substring(0, 4);
        }
        if (tr.getEventDate().getLte().length() >= MONTH_PRECISION_LENGTH) {
          // first 4 chars
          endMonth = tr.getEventDate().getLte().substring(5, 7);
        }
        if (tr.getEventDate().getLte().length() == DAY_PRECISION_LENGTH) {
          // first 4 chars
          endDay = tr.getEventDate().getLte().substring(8, 10);
        }

        if (((StringUtils.isEmpty(startDay) && StringUtils.isEmpty(endDay)
                || startDay.equals(endDay)))
            && StringUtils.isNotEmpty(startMonth)
            && startMonth.equals(endMonth)
            && StringUtils.isNotEmpty(startYear)
            && startYear.equals(endYear)) {
          determinedDatePrecision = MONTH_PRECISION;
        } else if (StringUtils.isEmpty(startDay) && StringUtils.isEmpty(endDay)) {
          determinedDatePrecision = MONTH_RANGE_PRECISION;
        }

        if (((StringUtils.isEmpty(startDay) && StringUtils.isEmpty(endDay))
                || startDay.equals(endDay))
            && (startMonth != endMonth
                || (StringUtils.isEmpty(startMonth) && StringUtils.isEmpty(endMonth)))
            && startYear == endYear
            && StringUtils.isNotEmpty(startYear)) {
          determinedDatePrecision = YEAR_PRECISION;
        } else if (StringUtils.isEmpty(startMonth) && StringUtils.isEmpty(endMonth)) {
          determinedDatePrecision = YEAR_RANGE_PRECISION;
        }

      } else if (tr.getEventDate().getGte() != null) {
        determinedDatePrecision = DAY_PRECISION;
        // single date
        if (tr.getEventDate().getGte().length() == MONTH_PRECISION_LENGTH) {
          determinedDatePrecision = MONTH_PRECISION;
        }
        if (tr.getEventDate().getGte().length() == YEAR_PRECISION_LENGTH) {
          determinedDatePrecision = YEAR_PRECISION;
        }
      } else {
        determinedDatePrecision = NOT_SUPPLIED;
      }

      tr.setDatePrecision(determinedDatePrecision);
    }
  }
}
