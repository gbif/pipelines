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
import java.util.function.Function;

import com.google.common.base.Strings;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
public interface ExtendedRecordInterpreter
    extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /** {@link DwcTerm#individualCount} interpretation. */
  static ExtendedRecordInterpreter interpretIndividualCount(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        SimpleTypeParser.parseInt(
                extendedRecord,
                DwcTerm.individualCount,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isPresent()) {
                    interpretedExtendedRecord.setIndividualCount(parseResult.get());
                  } else {
                    interpretation.withValidation(
                        Trace.of(
                            DwcTerm.individualCount.name(), IssueType.INDIVIDUAL_COUNT_INVALID));
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  static ExtendedRecordInterpreter interpretTypeStatus(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        VocabularyParsers.typeStatusParser()
            .map(
                extendedRecord,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isSuccessful()) {
                    interpretedExtendedRecord.setTypeStatus(parseResult.getPayload().name());
                  } else {
                    interpretation.withValidation(
                        Trace.of(DwcTerm.typeStatus.name(), IssueType.TYPE_STATUS_INVALID));
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  static ExtendedRecordInterpreter interpretLifeStage(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        VocabularyParsers.lifeStageParser()
            .map(
                extendedRecord,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isSuccessful()) {
                    interpretedExtendedRecord.setLifeStage(parseResult.getPayload().name());
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  static ExtendedRecordInterpreter interpretEstablishmentMeans(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        VocabularyParsers.establishmentMeansParser()
            .map(
                extendedRecord,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isSuccessful()) {
                    interpretedExtendedRecord.setEstablishmentMeans(
                        parseResult.getPayload().name());
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DwcTerm#sex} interpretation. */
  static ExtendedRecordInterpreter interpretSex(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        VocabularyParsers.sexParser()
            .map(
                extendedRecord,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isSuccessful()) {
                    interpretedExtendedRecord.setSex(parseResult.getPayload().name());
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  static ExtendedRecordInterpreter interpretBasisOfRecord(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
        VocabularyParsers.basisOfRecordParser()
            .map(
                extendedRecord,
                parseResult -> {
                  Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
                  if (parseResult.isSuccessful()) {
                    interpretedExtendedRecord.setBasisOfRecord(parseResult.getPayload().name());
                  } else {
                    interpretation.withValidation(
                        Trace.of(DwcTerm.basisOfRecord.name(), IssueType.BASIS_OF_RECORD_INVALID));
                  }
                  return interpretation;
                })
            .orElse(Interpretation.of(extendedRecord));
  }

  /** {@link DcTerm#references} interpretation. */
  static ExtendedRecordInterpreter interpretReferences(
      InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      String value = extendedRecord.getCoreTerms().get(DcTerm.references.qualifiedName());
      if (!Strings.isNullOrEmpty(value)) {
        URI parseResult = UrlParser.parse(value);
        if (Objects.isNull(parseResult)) {
          interpretation.withValidation(
              Trace.of(DcTerm.references.name(), IssueType.REFERENCES_URI_INVALID));
        } else {
          interpretedExtendedRecord.setReferences(parseResult.toString());
        }
      }
      return interpretation;
    };
  }
}
