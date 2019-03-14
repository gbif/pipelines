package org.gbif.pipelines.core.interpreters.extension;

import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.DeterminedDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.parsers.parsers.SimpleTypeParser;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Interpreter for the MeasurementsOrFacts extension, Interprets form {@link ExtendedRecord} to {@link
 * MeasurementOrFactRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MeasurementOrFactInterpreter {

  private static final TargetHandler<MeasurementOrFact> HANDLER =
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
          .map(DwcTerm.measurementDeterminedDate, MeasurementOrFactInterpreter::parseAndSetDeterminedDate);

  /**
   * Interprets measurements or facts of a {@link ExtendedRecord} and populates a {@link MeasurementOrFactRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, MeasurementOrFactRecord mfr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mfr);

    Result<MeasurementOrFact> result = HANDLER.convert(er);

    mfr.setMeasurementOrFactItems(result.getList());
    mfr.getIssues().setIssueList(result.getIssuesAsList());
  }

  /**
   * Parser for "http://rs.tdwg.org/dwc/terms/measurementDeterminedDate" term value
   */
  private static List<String> parseAndSetDeterminedDate(MeasurementOrFact mf, String v) {

    ParsedTemporal parsed = TemporalParser.parse(v);

    DeterminedDate determinedDate = new DeterminedDate();

    parsed.getFrom().map(Temporal::toString).ifPresent(determinedDate::setGte);
    parsed.getTo().map(Temporal::toString).ifPresent(determinedDate::setLte);

    mf.setDeterminedDateParsed(determinedDate);
    mf.setDeterminedDate(v);

    return parsed.getIssueList();
  }

  /**
   * Parser for "http://rs.tdwg.org/dwc/terms/measurementValue" term value, tries to parse if it is a Double
   */
  private static void parseAndSetValue(MeasurementOrFact mf, String v) {
    mf.setValue(v);
    Consumer<Optional<Double>> fn = result -> result.ifPresent(mf::setValueParsed);
    SimpleTypeParser.parseDouble(v, fn);
  }
}
