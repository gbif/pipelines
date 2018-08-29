package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.Context;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.multimedia.MultimediaParser;
import org.gbif.pipelines.core.parsers.multimedia.ParsedMultimedia;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

/** Interprets the multimedia terms of a {@link ExtendedRecord}. */
public class MultimediaInterpreter {

  private MultimediaInterpreter() {}

  public static Context<ExtendedRecord, MultimediaRecord> createContext(ExtendedRecord er) {
    MultimediaRecord mr = MultimediaRecord.newBuilder().setId(er.getId()).build();
    return new Context<>(er, mr);
  }

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link MultimediaRecord}
   * with the interpreted values.
   */
  public static void interpretMultimedia(ExtendedRecord er, MultimediaRecord mr) {

    // parse the multimedia fields of the ExtendedRecord
    ParsedField<List<ParsedMultimedia>> parsedResult = MultimediaParser.parseMultimedia(er);

    if (parsedResult.isSuccessful()) {
      List<Multimedia> multimediaList = new ArrayList<>();
      // convert the parsed results to the record

      // iterate through all the multimedias and set it to the multimediaRecord
      parsedResult
          .getResult()
          .forEach(
              parsedMultimedia -> {
                Multimedia multimedia = create(parsedMultimedia);

                // check against NP
                Optional.ofNullable(parsedMultimedia.getReferences())
                    .ifPresent(v -> multimedia.setReferences(v.toString()));
                Optional.ofNullable(parsedMultimedia.getCreated())
                    .ifPresent(v -> multimedia.setCreated(v.toString()));
                Optional.ofNullable(parsedMultimedia.getIdentifier())
                    .ifPresent(v -> multimedia.setIdentifier(v.toString()));

                multimediaList.add(multimedia);
              });

      // add parsed multimedia items to the record
      mr.setMultimediaItems(multimediaList);
    }

    // add issues
    addIssue(mr, parsedResult.getIssues());
  }

  private static Multimedia create(ParsedMultimedia parsedMultimedia) {
    return Multimedia.newBuilder()
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
  }
}
