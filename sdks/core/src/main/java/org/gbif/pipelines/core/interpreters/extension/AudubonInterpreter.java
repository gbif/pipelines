package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Interpreter for the Audubon extension, Interprets form {@link ExtendedRecord} to {@link AudubonRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ac/audubon.xml</a>
 */
public class AudubonInterpreter {

  private static final TargetHandler<Audubon> HANDLER =
      ExtensionInterpretation.extenstion(Extension.AUDUBON)
          .to(Audubon::new);

  private AudubonInterpreter() {}

  /**
   * Interprets audubons of a {@link ExtendedRecord} and populates a {@link AudubonRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, AudubonRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Audubon> result = HANDLER.convert(er);

    ar.setAudubonItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }
}
