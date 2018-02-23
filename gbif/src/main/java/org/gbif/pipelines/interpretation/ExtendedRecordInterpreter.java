package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.interpretation.parsers.SimpleTypeParser;
import org.gbif.pipelines.interpretation.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to it.
 */
public interface ExtendedRecordInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * {@link DwcTerm#individualCount} interpretation.
   */
  static ExtendedRecordInterpreter interpretIndividualCount(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      SimpleTypeParser.parseInt(extendedRecord, DwcTerm.individualCount, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if(parseResult.isPresent()) {
          interpretedExtendedRecord.setIndividualCount(parseResult.get());
        } else {
          interpretation.withValidation(Arrays.asList(Interpretation.Trace.of(DwcTerm.individualCount.name(),IssueType.INDIVIDUAL_COUNT_INVALID)));
        }
        return interpretation;
      });
  }

  /**
   * {@link DwcTerm#typeStatus} interpretation.
   */
  static ExtendedRecordInterpreter interpretTypeStatus(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .typeStatusParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            interpretedExtendedRecord.setTypeStatus(parseResult.getPayload().name());
          } else {
            interpretation.withValidation(Arrays.asList(Interpretation.Trace.of(DwcTerm.typeStatus.name(),IssueType.TYPE_STATUS_INVALID)));
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#lifeStage} interpretation.
   */
  static ExtendedRecordInterpreter interpretLifeStage(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .lifeStageParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            interpretedExtendedRecord.setLifeStage(parseResult.getPayload().name());
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#establishmentMeans} interpretation.
   */
  static ExtendedRecordInterpreter interpretEstablishmentMeans(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .establishmentMeansParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            interpretedExtendedRecord.setEstablishmentMeans(parseResult.getPayload().name());
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#sex} interpretation.
   */
  static ExtendedRecordInterpreter interpretSex(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .sexParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            interpretedExtendedRecord.setSex(parseResult.getPayload().name());
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#basisOfRecord} interpretation.
   */
  static ExtendedRecordInterpreter interpretBasisOfRecord(InterpretedExtendedRecord interpretedExtendedRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .basisOfRecordParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            interpretedExtendedRecord.setBasisOfRecord(parseResult.getPayload().name());
          } else {
            interpretation.withValidation(Arrays.asList(Interpretation.Trace.of(DwcTerm.basisOfRecord.name(), IssueType.BASIS_OF_RECORD_INVALID)));
          }
          return interpretation;
        }).get();
  }
}
