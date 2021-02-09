package org.gbif.pipelines.core.interpreters.extension;

import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.parsers.temporal.EventRange;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.TemporalRangeParser;
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
          .map(DwcTerm.measurementValue, MeasurementOrFact::setValue)
          .map(DwcTerm.measurementDeterminedDate, this::parseAndSetDeterminedDate);

  private final TemporalRangeParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private MeasurementOrFactInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.temporalParser =
        TemporalRangeParser.builder().temporalParser(TemporalParser.create(orderings)).create();
    this.preprocessDateFn = preprocessDateFn;
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

  /** Parser for "http://rs.tdwg.org/dwc/terms/measurementDeterminedDate" term value */
  private void parseAndSetDeterminedDate(MeasurementOrFact mf, String v) {
    String normalizedDate = Optional.ofNullable(preprocessDateFn).map(x -> x.apply(v)).orElse(v);
    EventRange eventRange = temporalParser.parse(normalizedDate);

    DeterminedDate determinedDate = new DeterminedDate();
    eventRange.getFrom().map(TemporalAccessor::toString).ifPresent(determinedDate::setGte);
    eventRange.getTo().map(TemporalAccessor::toString).ifPresent(determinedDate::setLte);

    mf.setDeterminedDate(determinedDate);
  }
}
