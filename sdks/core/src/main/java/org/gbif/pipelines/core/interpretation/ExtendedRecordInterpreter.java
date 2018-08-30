package org.gbif.pipelines.core.interpretation;

import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.Context;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;

import java.net.URI;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.TYPE_STATUS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
public class ExtendedRecordInterpreter {

  private ExtendedRecordInterpreter() {}

  public static Context<ExtendedRecord, InterpretedExtendedRecord> createContext(
      ExtendedRecord er) {
    InterpretedExtendedRecord ier =
        InterpretedExtendedRecord.newBuilder().setId(er.getId()).build();
    return new Context<>(er, ier);
  }

  /** {@link DwcTerm#individualCount} interpretation. */
  public static void interpretIndividualCount(ExtendedRecord er, InterpretedExtendedRecord ier) {
    SimpleTypeParser.parseInt(
        er,
        DwcTerm.individualCount,
        parseResult -> {
          if (parseResult.isPresent()) {
            ier.setIndividualCount(parseResult.get());
          } else {
            addIssue(ier, INDIVIDUAL_COUNT_INVALID);
          }
        });
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static void interpretTypeStatus(ExtendedRecord er, InterpretedExtendedRecord ier) {
    VocabularyParsers.typeStatusParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                ier.setTypeStatus(parseResult.getPayload().name());
              } else {
                addIssue(ier, TYPE_STATUS_INVALID);
              }
              return ier;
            });
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  public static void interpretLifeStage(ExtendedRecord er, InterpretedExtendedRecord ier) {
    VocabularyParsers.lifeStageParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                ier.setLifeStage(parseResult.getPayload().name());
              }
              return ier;
            });
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  public static void interpretEstablishmentMeans(ExtendedRecord er, InterpretedExtendedRecord ier) {
    VocabularyParsers.establishmentMeansParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                ier.setEstablishmentMeans(parseResult.getPayload().name());
              }
              return ier;
            });
  }

  /** {@link DwcTerm#sex} interpretation. */
  public static void interpretSex(ExtendedRecord er, InterpretedExtendedRecord ier) {
    VocabularyParsers.sexParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                ier.setSex(parseResult.getPayload().name());
              }
              return ier;
            });
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  public static void interpretBasisOfRecord(ExtendedRecord er, InterpretedExtendedRecord ier) {
    VocabularyParsers.basisOfRecordParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                ier.setBasisOfRecord(parseResult.getPayload().name());
              } else {
                addIssue(ier, BASIS_OF_RECORD_INVALID);
              }
              return ier;
            });
  }

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(ExtendedRecord er, InterpretedExtendedRecord ier) {
    String value = extractValue(er, DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        ier.setReferences(parseResult.toString());
      } else {
        addIssue(ier, REFERENCES_URI_INVALID);
      }
    }
  }
}
