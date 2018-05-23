package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.multimedia.MultimediaParser;
import org.gbif.pipelines.core.parsers.multimedia.ParsedMultimedia;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interprets the multimedia terms of a {@link ExtendedRecord}.
 */
public interface MultimediaInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link MultimediaRecord} with the
   * interpreted values.
   */
  static MultimediaInterpreter interpretMultimedia(MultimediaRecord multimediaRecord) {
    return (ExtendedRecord extendedRecord) -> {

      // parse the multimedia fields of the ExtendedRecord
      ParsedField<List<ParsedMultimedia>> parsedResult = MultimediaParser.parseMultimedia(extendedRecord);

      if (parsedResult.isSuccessful()) {
        List<Multimedia> multimediaList = new ArrayList<>();
        // convert the parsed results to the record

        // iterate through all the multimedias and set it to the multimediaRecord
        parsedResult.getResult().forEach(parsedMultimedia -> {

          Multimedia multimedia = Multimedia.newBuilder()
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
            .ifPresent(value -> multimedia.setReferences(value.toString()));
          Optional.ofNullable(parsedMultimedia.getCreated())
            .ifPresent(value -> multimedia.setCreated(value.toString()));
          Optional.ofNullable(parsedMultimedia.getIdentifier())
            .ifPresent(value -> multimedia.setIdentifier(value.toString()));

          multimediaList.add(multimedia);
        });

        // add parsed multimedia items to the record
        multimediaRecord.setMultimediaItems(multimediaList);
      }

      // create the interpretation instance
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // add issues
      parsedResult.getIssues().forEach(issue -> {
        Interpretation.Trace<IssueType> trace;
        // check if a term should be used to create the trace
        if (Objects.nonNull(issue.getTerms()) && !issue.getTerms().isEmpty() && Objects.nonNull(issue.getTerms()
                                                                                                  .get(0))) {
          // FIXME: now we take the first term. Should Trace accept a list of terms??
          trace = Interpretation.Trace.of(issue.getTerms().get(0).simpleName(), issue.getIssueType());
        } else {
          trace = Interpretation.Trace.of(issue.getIssueType());
        }

        interpretation.withValidation(trace);
      });

      return interpretation;
    };
  }

}
