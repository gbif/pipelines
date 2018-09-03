package org.gbif.pipelines.core.interpreters;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.multimedia.MultimediaParser;
import org.gbif.pipelines.parsers.parsers.multimedia.ParsedMultimedia;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;

/** Interprets the multimedia terms of a {@link ExtendedRecord}. */
public class MultimediaInterpreter {

  private MultimediaInterpreter() {}

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link MultimediaRecord}
   * with the interpreted values.
   */
  public static void interpretMultimedia(ExtendedRecord er, MultimediaRecord mr) {

    // parse the multimedia fields of the ExtendedRecord
    ParsedField<List<ParsedMultimedia>> parsedResult = MultimediaParser.parseMultimedia(er);

    if (parsedResult.isSuccessful()) {
      // convert the parsed results to the record

      // iterate through all the multimedias and set it to the multimediaRecord
      List<Multimedia> multimediaList =
          parsedResult
              .getResult()
              .stream()
              .map(MultimediaInterpreter::create)
              .collect(Collectors.toList());

      // add parsed multimedia items to the record
      mr.setMultimediaItems(multimediaList);
    }

    // add issues
    addIssue(mr, parsedResult.getIssues());
  }

  private static Multimedia create(ParsedMultimedia parsedMultimedia) {
    Multimedia multimedia =
        Multimedia.newBuilder()
            .setAudience(parsedMultimedia.getAudience())
            .setContributor(parsedMultimedia.getContributor())
            .setCreator(parsedMultimedia.getCreator())
            .setDescription(parsedMultimedia.getDescription())
            .setFormat(parsedMultimedia.getFormat())
            .setLicense(parsedMultimedia.getLicense())
            .setPublisher(parsedMultimedia.getPublisher())
            .setRightsHolder(parsedMultimedia.getRightsHolder())
            .setSource(parsedMultimedia.getSource())
            .setTitle(parsedMultimedia.getTitle())
            .setType(parsedMultimedia.getType())
            .build();

    // check against NP
    Optional.ofNullable(parsedMultimedia.getReferences())
        .ifPresent(v -> multimedia.setReferences(v.toString()));
    Optional.ofNullable(parsedMultimedia.getCreated())
        .ifPresent(v -> multimedia.setCreated(v.toString()));
    Optional.ofNullable(parsedMultimedia.getIdentifier())
        .ifPresent(v -> multimedia.setIdentifier(v.toString()));

    return multimedia;
  }
}
