package org.gbif.pipelines.core.interpreters;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.parsers.SimpleTypeParser;
import org.gbif.pipelines.parsers.parsers.VocabularyParsers;

import java.net.URI;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.TYPE_STATUS_INVALID;
import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
public class BasicInterpreter {

  private BasicInterpreter() {}

  /** {@link DwcTerm#individualCount} interpretation. */
  public static void interpretIndividualCount(ExtendedRecord er, BasicRecord br) {

    Consumer<Optional<Integer>> fn =
        parseResult -> {
          if (parseResult.isPresent()) {
            br.setIndividualCount(parseResult.get());
          } else {
            addIssue(br, INDIVIDUAL_COUNT_INVALID);
          }
        };

    SimpleTypeParser.parseInt(er, DwcTerm.individualCount, fn);
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static void interpretTypeStatus(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<TypeStatus>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setTypeStatus(parseResult.getPayload().name());
          } else {
            addIssue(br, TYPE_STATUS_INVALID);
          }
          return br;
        };

    VocabularyParsers.typeStatusParser().map(er, fn);
  }

  /** {@link DwcTerm#lifeStage} interpretation. */
  public static void interpretLifeStage(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<LifeStage>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setLifeStage(parseResult.getPayload().name());
          }
          return br;
        };

    VocabularyParsers.lifeStageParser().map(er, fn);
  }

  /** {@link DwcTerm#establishmentMeans} interpretation. */
  public static void interpretEstablishmentMeans(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<EstablishmentMeans>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setEstablishmentMeans(parseResult.getPayload().name());
          }
          return br;
        };

    VocabularyParsers.establishmentMeansParser().map(er, fn);
  }

  /** {@link DwcTerm#sex} interpretation. */
  public static void interpretSex(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<Sex>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setSex(parseResult.getPayload().name());
          }
          return br;
        };

    VocabularyParsers.sexParser().map(er, fn);
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  public static void interpretBasisOfRecord(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<BasisOfRecord>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setBasisOfRecord(parseResult.getPayload().name());
          } else {
            addIssue(br, BASIS_OF_RECORD_INVALID);
          }
          return br;
        };

    VocabularyParsers.basisOfRecordParser().map(er, fn);
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
