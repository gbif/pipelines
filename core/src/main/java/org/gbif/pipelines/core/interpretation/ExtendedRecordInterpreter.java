package org.gbif.pipelines.core.interpretation;

import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.issue.IssueType;

import java.net.URI;
import java.util.Objects;
import java.util.function.BiConsumer;

import com.google.common.base.Strings;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
public interface ExtendedRecordInterpreter
    extends BiConsumer<ExtendedRecord, Interpretation<InterpretedExtendedRecord>> {

  static ExtendedRecordInterpreter interpretId() {
    return (extendedRecord, interpretation) ->
        interpretation.getValue().setId(extendedRecord.getId());
  }

  /** {@link DwcTerm#individualCount} interpretation. */
  static ExtendedRecordInterpreter interpretIndividualCount() {
    return (extendedRecord, interpretation) ->
        SimpleTypeParser.parseInt(
            extendedRecord,
            DwcTerm.individualCount,
            parseResult -> {
              if (parseResult.isPresent()) {
                interpretation.getValue().setIndividualCount(parseResult.get());
              } else {
                interpretation.withValidation(
                    Trace.of(DwcTerm.individualCount.name(), IssueType.INDIVIDUAL_COUNT_INVALID));
              }
              return interpretation;
            });
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  static ExtendedRecordInterpreter interpretTypeStatus() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.typeStatusParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation.getValue().setTypeStatus(parseResult.getPayload().name());
                  } else {
                    interpretation.withValidation(
                        Trace.of(DwcTerm.typeStatus.name(), IssueType.TYPE_STATUS_INVALID));
                  }
                  return interpretation;
                });
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  static ExtendedRecordInterpreter interpretLifeStage() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.lifeStageParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation.getValue().setLifeStage(parseResult.getPayload().name());
                  }
                  return interpretation;
                });
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  static ExtendedRecordInterpreter interpretEstablishmentMeans() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.establishmentMeansParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation
                        .getValue()
                        .setEstablishmentMeans(parseResult.getPayload().name());
                  }
                  return interpretation;
                });
  }

  /** {@link DwcTerm#sex} interpretation. */
  static ExtendedRecordInterpreter interpretSex() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.sexParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation.getValue().setSex(parseResult.getPayload().name());
                  }
                  return interpretation;
                });
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  static ExtendedRecordInterpreter interpretBasisOfRecord() {
    return (extendedRecord, interpretation) ->
        VocabularyParsers.basisOfRecordParser()
            .map(
                extendedRecord,
                parseResult -> {
                  if (parseResult.isSuccessful()) {
                    interpretation.getValue().setBasisOfRecord(parseResult.getPayload().name());
                  } else {
                    interpretation.withValidation(
                        Trace.of(DwcTerm.basisOfRecord.name(), IssueType.BASIS_OF_RECORD_INVALID));
                  }
                  return interpretation;
                });
  }

  /** {@link DcTerm#references} interpretation. */
  static ExtendedRecordInterpreter interpretReferences() {
    return (extendedRecord, interpretation) -> {
      String value = extendedRecord.getCoreTerms().get(DcTerm.references.qualifiedName());
      if (!Strings.isNullOrEmpty(value)) {
        URI parseResult = UrlParser.parse(value);
        if (Objects.nonNull(parseResult)) {
          interpretation.getValue().setReferences(parseResult.toString());
        } else {
          interpretation.withValidation(
              Trace.of(DcTerm.references.name(), IssueType.REFERENCES_URI_INVALID));
        }
      }
    };
  }
}
