package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.Amplification;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public class AmplificationInterpreter {

  public static final String EXTENSION_ROW_TYPE = "http://rs.gbif.org/extension/ggbn/amplification.xml";

  private static final TargetHandler<Amplification> HANDLER =
      ExtensionInterpretation.extenstion(EXTENSION_ROW_TYPE)
          .to(Amplification::new);

  private AmplificationInterpreter() {}

  public static void interpret(ExtendedRecord er, AmplificationRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Amplification> result = HANDLER.convert(er);

    ar.setItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }

}
