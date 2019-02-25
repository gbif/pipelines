package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;

/**
 * Interpreter for the MeasurementsOrFacts extension, Interprets form {@link ExtendedRecord} to {@link
 * MeasurementOrFactRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
public class MeasurementOrFactInterpreter {

  private static final TargetHandler<MeasurementOrFact> HANDLER =
      ExtensionInterpretation.extenstion(Extension.MEASUREMENT_OR_FACT)
          .to(MeasurementOrFact::new);

  private MeasurementOrFactInterpreter() {}

  /**
   * Interprets measurements or facts of a {@link ExtendedRecord} and populates a {@link ImageRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, MeasurementOrFactRecord mfr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mfr);

    Result<MeasurementOrFact> result = HANDLER.convert(er);

    mfr.setMeasurementOrFactItems(result.getList());
    mfr.getIssues().setIssueList(result.getIssuesAsList());
  }

}
