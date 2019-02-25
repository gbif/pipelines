package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;


public class AudubonInterpreter {

  private static final TargetHandler<Audubon> HANDLER =
      ExtensionInterpretation.extenstion(Extension.AUDUBON)
          .to(Audubon::new);

  private AudubonInterpreter() {}

  public static void interpret(ExtendedRecord er, AudubonRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Audubon> result = HANDLER.convert(er);

    ar.setItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }
}
