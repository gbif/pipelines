package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;

public class MeasurementOrFactInterpreter {

  private static final TargetHandler<MeasurementOrFact> HANDLER =
      ExtensionInterpretation.extenstion(Extension.MEASUREMENT_OR_FACT)
          .to(MeasurementOrFact::new);

  private MeasurementOrFactInterpreter() {}

  public static void interpret(ExtendedRecord er, MeasurementOrFactRecord mfr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mfr);

    Result<MeasurementOrFact> result = HANDLER.convert(er);

    mfr.setItems(result.getList());
    mfr.getIssues().setIssueList(result.getIssuesAsList());
  }

}
