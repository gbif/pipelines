package org.gbif.pipelines.core.interpreters.extension;

import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.DeterminedDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;

/**
 * Interpreter for the MeasurementsOrFacts extension, Interprets form {@link ExtendedRecord} to
 * {@link MeasurementOrFactRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
public class MeasurementOrFactInterpreter {

  private final TargetHandler<MeasurementOrFact> handler =
      ExtensionInterpretation.extension(Extension.MEASUREMENT_OR_FACT)
          .to(MeasurementOrFact::new)
          .map(DwcTerm.measurementID, MeasurementOrFact::setId)
          .map(DwcTerm.measurementType, MeasurementOrFact::setType)
          .map(DwcTerm.measurementAccuracy, MeasurementOrFact::setAccuracy)
          .map(DwcTerm.measurementUnit, MeasurementOrFact::setUnit)
          .map(DwcTerm.measurementDeterminedBy, MeasurementOrFact::setDeterminedBy)
          .map(DwcTerm.measurementMethod, MeasurementOrFact::setMethod)
          .map(DwcTerm.measurementRemarks, MeasurementOrFact::setRemarks)
          .map(DwcTerm.measurementValue, MeasurementOrFactInterpreter::parseAndSetValue)
          .map(DwcTerm.measurementDeterminedDate, this::parseAndSetDeterminedDate);

  private final TemporalParser temporalParser;

  private MeasurementOrFactInterpreter(DateComponentOrdering dateComponentOrdering) {
    this.temporalParser = TemporalParser.create(dateComponentOrdering);
  }

  public static MeasurementOrFactInterpreter create(DateComponentOrdering dateComponentOrdering) {
    return new MeasurementOrFactInterpreter(dateComponentOrdering);
  }

  public static MeasurementOrFactInterpreter create() {
    return create(null);
  }

  /**
   * Interprets measurements or facts of a {@link ExtendedRecord} and populates a {@link
   * MeasurementOrFactRecord} with the interpreted values.
   */
  public void interpret(ExtendedRecord er, MeasurementOrFactRecord mfr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mfr);

    Result<MeasurementOrFact> result = handler.convert(er);

    mfr.setMeasurementOrFactItems(result.getList());
    mfr.getIssues().setIssueList(result.getIssuesAsList());
  }

  /**
   * Parser for "http://rs.tdwg.org/dwc/terms/measurementValue" term value, tries to parse if it is
   * a Double
   */
  private static void parseAndSetValue(MeasurementOrFact mf, String v) {
    mf.setValue(v);
    Consumer<Optional<Double>> fn =
        result ->
            result.ifPresent(
                x -> {
                  if (!x.isInfinite() && !x.isNaN()) {
                    mf.setValueParsed(x);
                  }
                });
    SimpleTypeParser.parseDouble(v, fn);
  }

  /** Parser for "http://rs.tdwg.org/dwc/terms/measurementDeterminedDate" term value */
  private void parseAndSetDeterminedDate(MeasurementOrFact mf, String v) {
    DeterminedDate determinedDate = new DeterminedDate();

    OccurrenceParseResult<TemporalAccessor> result = temporalParser.parseRecordedDate(v);
    if (result.isSuccessful()) {
      determinedDate.setGte(result.getPayload().toString());
    }

    mf.setDeterminedDateParsed(determinedDate);
    mf.setDeterminedDate(v);
  }
}
