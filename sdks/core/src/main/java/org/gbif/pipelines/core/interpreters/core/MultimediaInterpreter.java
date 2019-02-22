package org.gbif.pipelines.core.interpreters.core;

import java.util.List;
import java.util.Objects;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.multimedia.MultimediaParser;
import org.gbif.pipelines.parsers.utils.ModelUtils;

/** Interprets the multimedia terms of a {@link ExtendedRecord}. */
public class MultimediaInterpreter {

  private MultimediaInterpreter() {}

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link MultimediaRecord}
   * with the interpreted values.
   */
  public static void interpretMultimedia(ExtendedRecord er, MultimediaRecord mr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mr);

    // parse the multimedia fields of the ExtendedRecord
    ParsedField<List<Multimedia>> parsedResult = MultimediaParser.parseMultimedia(er);

    if (parsedResult.isSuccessful()) {
      // add parsed multimedia items to the record
      mr.setMultimediaItems(parsedResult.getResult());
    }

    // add issues
    ModelUtils.addIssue(mr, parsedResult.getIssues());
  }
}
