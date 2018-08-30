package org.gbif.pipelines.core.interpreter;

import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

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
public class BasicInterpreter {

  private BasicInterpreter() {}

  /** {@link DwcTerm#individualCount} interpretation. */
  public static void interpretIndividualCount(ExtendedRecord er, BasicRecord br) {
    SimpleTypeParser.parseInt(
        er,
        DwcTerm.individualCount,
        parseResult -> {
          if (parseResult.isPresent()) {
            br.setIndividualCount(parseResult.get());
          } else {
            addIssue(br, INDIVIDUAL_COUNT_INVALID);
          }
        });
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static void interpretTypeStatus(ExtendedRecord er, BasicRecord br) {
    VocabularyParsers.typeStatusParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                br.setTypeStatus(parseResult.getPayload().name());
              } else {
                addIssue(br, TYPE_STATUS_INVALID);
              }
              return br;
            });
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  public static void interpretLifeStage(ExtendedRecord er, BasicRecord br) {
    VocabularyParsers.lifeStageParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                br.setLifeStage(parseResult.getPayload().name());
              }
              return br;
            });
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  public static void interpretEstablishmentMeans(ExtendedRecord er, BasicRecord br) {
    VocabularyParsers.establishmentMeansParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                br.setEstablishmentMeans(parseResult.getPayload().name());
              }
              return br;
            });
  }

  /** {@link DwcTerm#sex} interpretation. */
  public static void interpretSex(ExtendedRecord er, BasicRecord br) {
    VocabularyParsers.sexParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                br.setSex(parseResult.getPayload().name());
              }
              return br;
            });
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  public static void interpretBasisOfRecord(ExtendedRecord er, BasicRecord br) {
    VocabularyParsers.basisOfRecordParser()
        .map(
            er,
            parseResult -> {
              if (parseResult.isSuccessful()) {
                br.setBasisOfRecord(parseResult.getPayload().name());
              } else {
                addIssue(br, BASIS_OF_RECORD_INVALID);
              }
              return br;
            });
  }

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(ExtendedRecord er, BasicRecord br) {
    String value = extractValue(er, DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        br.setReferences(parseResult.toString());
      } else {
        addIssue(br, REFERENCES_URI_INVALID);
      }
    }
  }
}
