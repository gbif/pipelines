package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.Extension.AMPLIFICATION;
import static org.gbif.api.vocabulary.Extension.CLONING;
import static org.gbif.api.vocabulary.Extension.DNA_DERIVED_DATA;
import static org.gbif.api.vocabulary.Extension.GEL_IMAGE;
import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_BASIS_OF_RECORD;
import static org.gbif.api.vocabulary.OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT;
import static org.gbif.api.vocabulary.OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE;
import static org.gbif.pipelines.core.utils.ModelUtils.DEFAULT_SEPARATOR;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.identifier.AgentIdentifierParser;
import org.gbif.pipelines.core.interpreters.model.BasicRecord;
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;

/**
 * Interpreting function that receives a Record instance and applies an interpretation to
 * it.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BasicInterpreter {

  private static final Parsable<String> TYPE_NAME_PARSER =
      org.gbif.common.parsers.TypifiedNameParser.getInstance();


  public static void interpret(ExtendedRecord er, BasicRecord br, VocabularyService vocabularyService, KeyValueStore<String, OccurrenceStatus> occStatusKvStore) {
          Interpretation.from(er).to(br)
            .via(BasicInterpreter::interpretBasisOfRecord)
            .via(BasicInterpreter::interpretTypifiedName)
            .via(VocabularyInterpreter.interpretSex(vocabularyService))
            .via(VocabularyInterpreter.interpretTypeStatus(vocabularyService))
            .via(BasicInterpreter::interpretIndividualCount)
            .via((e, r) -> CoreInterpreter.interpretReferences(e, r, r::setReferences))
            .via(BasicInterpreter::interpretOrganismQuantity)
            .via(BasicInterpreter::interpretOrganismQuantityType)
            .via((e, r) -> CoreInterpreter.interpretSampleSizeUnit(e, r::setSampleSizeUnit))
            .via((e, r) -> CoreInterpreter.interpretSampleSizeValue(e, r::setSampleSizeValue))
            .via(BasicInterpreter::interpretRelativeOrganismQuantity)
            .via((e, r) -> CoreInterpreter.interpretLicense(e, r::setLicense))
            .via(BasicInterpreter::interpretIdentifiedByIds)
            .via(BasicInterpreter::interpretRecordedByIds)
            .via(BasicInterpreter.interpretOccurrenceStatus(occStatusKvStore))
            .via(VocabularyInterpreter.interpretEstablishmentMeans(vocabularyService))
            .via(VocabularyInterpreter.interpretLifeStage(vocabularyService))
            .via(VocabularyInterpreter.interpretPathway(vocabularyService))
            .via(VocabularyInterpreter.interpretDegreeOfEstablishment(vocabularyService))
            .via((e, r) -> CoreInterpreter.interpretDatasetID(e, r::setDatasetID))
            .via((e, r) -> CoreInterpreter.interpretDatasetName(e, r::setDatasetName))
            .via(BasicInterpreter::interpretOtherCatalogNumbers)
            .via(BasicInterpreter::interpretRecordedBy)
            .via(BasicInterpreter::interpretIdentifiedBy)
            .via(BasicInterpreter::interpretPreparations)
            .via((e, r) -> CoreInterpreter.interpretSamplingProtocol(e, r::setSamplingProtocol))
            .via(BasicInterpreter::interpretProjectId)
            .via(BasicInterpreter::interpretIsSequenced)
            .via(BasicInterpreter::interpretAssociatedSequences).getOfNullable();
  }


  /** {@link DwcTerm#individualCount} interpretation. */
  public static void interpretIndividualCount(ExtendedRecord er, BasicRecord br) {

    Consumer<Optional<Integer>> fn =
        parseResult -> {
          if (parseResult.isPresent()) {
            br.setIndividualCount(parseResult.get());
          } else {
            br.addIssue(INDIVIDUAL_COUNT_INVALID);
          }
        };

    SimpleTypeParser.parsePositiveInt(er, DwcTerm.individualCount, fn);
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  public static void interpretBasisOfRecord(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<BasisOfRecord>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setBasisOfRecord(parseResult.getPayload().name());
          } else {
            br.setBasisOfRecord(BasisOfRecord.OCCURRENCE.name());
            br.addIssue(BASIS_OF_RECORD_INVALID);
          }
          return br;
        };

    VocabularyParser.basisOfRecordParser().map(er, fn);

    if (br.getBasisOfRecord() == null || br.getBasisOfRecord().isEmpty()) {
      br.setBasisOfRecord(BasisOfRecord.OCCURRENCE.name());
      br.addIssue(BASIS_OF_RECORD_INVALID);
    }
  }

  /** {@link GbifTerm#typifiedName} interpretation. */
  public static void interpretTypifiedName(ExtendedRecord er, BasicRecord br) {
    Optional<String> typifiedName = er.extractOptValue(GbifTerm.typifiedName);
    if (typifiedName.isPresent()) {
      br.setTypifiedName(typifiedName.get());
    } else {
      Optional.ofNullable(er.extractOptValue(DwcTerm.typeStatus))
          .ifPresent(
              typeStatusValue -> {
                ParseResult<String> result =
                    TYPE_NAME_PARSER.parse(er.extractNullAwareValue(DwcTerm.typeStatus));
                if (result.isSuccessful()) {
                  br.setTypifiedName(result.getPayload());
                }
              });
    }
  }

  /** {@link DwcTerm#organismQuantity} interpretation. */
  public static void interpretOrganismQuantity(ExtendedRecord er, BasicRecord br) {
    er.extractOptValue(DwcTerm.organismQuantity)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(br::setOrganismQuantity);
  }

  /** {@link DwcTerm#organismQuantityType} interpretation. */
  public static void interpretOrganismQuantityType(ExtendedRecord er, BasicRecord br) {
    er.extractOptValue(DwcTerm.organismQuantityType)
        .map(String::trim)
        .ifPresent(br::setOrganismQuantityType);
  }

  /**
   * If the organism and sample have the same measure type, we can calculate relative organism
   * quantity
   */
  public static void interpretRelativeOrganismQuantity(BasicRecord br) {
    if (!Strings.isNullOrEmpty(br.getOrganismQuantityType())
        && !Strings.isNullOrEmpty(br.getSampleSizeUnit())
        && br.getOrganismQuantityType().equalsIgnoreCase(br.getSampleSizeUnit())) {
      Double organismQuantity = br.getOrganismQuantity();
      Double sampleSizeValue = br.getSampleSizeValue();
      if (organismQuantity != null && sampleSizeValue != null) {
        double result = organismQuantity / sampleSizeValue;
        if (!Double.isNaN(result) && !Double.isInfinite(result)) {
          br.setRelativeOrganismQuantity(organismQuantity / sampleSizeValue);
        }
      }
    }
  }

  /** {@link DwcTerm#identifiedByID}. */
  public static void interpretIdentifiedByIds(ExtendedRecord er, BasicRecord br) {
    er.extractOptValue(DwcTerm.identifiedByID)
        .filter(x -> !x.isEmpty())
        .map(AgentIdentifierParser::parse)
        .map(ArrayList::new)
        .ifPresent(br::setIdentifiedByIds);
  }

  /** {@link DwcTerm#recordedByID} interpretation. */
  public static void interpretRecordedByIds(ExtendedRecord er, BasicRecord br) {
    er.extractOptValue(DwcTerm.recordedByID)
        .filter(x -> !x.isEmpty())
        .map(AgentIdentifierParser::parse)
        .map(ArrayList::new)
        .ifPresent(br::setRecordedByIds);
  }

  /** {@link DwcTerm#occurrenceStatus} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretOccurrenceStatus(
      KeyValueStore<String, OccurrenceStatus> occStatusKvStore) {
    return (er, br) -> {
      if (occStatusKvStore == null) {
        return;
      }

      String rawCount = er.extractNullAwareValue(DwcTerm.individualCount);
      Integer parsedCount = SimpleTypeParser.parsePositiveIntOpt(rawCount).orElse(null);

      String rawOccStatus = er.extractNullAwareValue(DwcTerm.occurrenceStatus);
      OccurrenceStatus parsedOccStatus =
          rawOccStatus != null ? occStatusKvStore.get(rawOccStatus) : null;

      boolean isCountNull = rawCount == null;
      boolean isCountRubbish = rawCount != null && parsedCount == null;
      boolean isCountZero = parsedCount != null && parsedCount == 0;
      boolean isCountGreaterZero = parsedCount != null && parsedCount > 0;

      boolean isOccNull = rawOccStatus == null;
      boolean isOccPresent = parsedOccStatus == OccurrenceStatus.PRESENT;
      boolean isOccAbsent = parsedOccStatus == OccurrenceStatus.ABSENT;
      boolean isOccRubbish = parsedOccStatus == null;

      // https://github.com/gbif/pipelines/issues/392
      boolean isSpecimen =
          Optional
              .ofNullable(br.getBasisOfRecord())
              .map(BasisOfRecord::valueOf)
              .map(
                  x ->
                      x == BasisOfRecord.PRESERVED_SPECIMEN
                          || x == BasisOfRecord.FOSSIL_SPECIMEN
                          || x == BasisOfRecord.LIVING_SPECIMEN)
              .orElse(false);

      // rawCount === null
      if (isCountNull) {
        if (isOccNull || isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(OCCURRENCE_STATUS_UNPARSABLE);
        }
      } else if (isCountRubbish) {
        if (isOccNull || isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(OCCURRENCE_STATUS_UNPARSABLE);
        }
        br.addIssue(INDIVIDUAL_COUNT_INVALID);
      } else if (isCountZero) {
        if (isOccNull && isSpecimen) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(OCCURRENCE_STATUS_INFERRED_FROM_BASIS_OF_RECORD);
        } else if (isOccNull) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          br.addIssue(OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        } else if (isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS);
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          br.addIssue(OCCURRENCE_STATUS_UNPARSABLE);
          br.addIssue(OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        }
      } else if (isCountGreaterZero) {
        if (isOccNull) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        } else if (isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          br.addIssue(INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS);
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          br.addIssue(OCCURRENCE_STATUS_UNPARSABLE);
          br.addIssue(OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        }
      }
    };
  }

  /** {@link DwcTerm#otherCatalogNumbers} interpretation. */
  public static void interpretOtherCatalogNumbers(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(DEFAULT_SEPARATOR + "|;",  DwcTerm.otherCatalogNumbers);
    if (!list.isEmpty()) {
      br.setOtherCatalogNumbers(list);
    }
  }

  /** {@link DwcTerm#recordedBy} interpretation. */
  public static void interpretRecordedBy(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(DwcTerm.recordedBy);
    if (!list.isEmpty()) {
      br.setRecordedBy(list);
    }
  }

  /** {@link DwcTerm#identifiedBy} interpretation. */
  public static void interpretIdentifiedBy(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(DwcTerm.identifiedBy);
    if (!list.isEmpty()) {
      br.setIdentifiedBy(list);
    }
  }

  /** {@link DwcTerm#preparations} interpretation. */
  public static void interpretPreparations(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(DwcTerm.preparations);
    if (!list.isEmpty()) {
      br.setPreparations(list);
    }
  }

  /** {@link org.gbif.dwc.terms.GbifTerm#projectId} interpretation. */
  public static void interpretProjectId(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(GbifTerm.projectId);
    if (!list.isEmpty()) {
      br.setProjectId(list);
    }
  }

  /** {@link DwcTerm#associatedSequences} interpretation. */
  public static void interpretIsSequenced(ExtendedRecord er, BasicRecord br) {

    boolean hasExt = false;
    var extensions = er.getExtensions();

    if (extensions != null) {
      Predicate<Extension> fn =
          ext -> {
            var e = extensions.get(ext.getRowType());
            return e != null && !e.isEmpty();
          };
      hasExt = fn.test(DNA_DERIVED_DATA);
      hasExt = fn.test(AMPLIFICATION) || hasExt;
      hasExt = fn.test(CLONING) || hasExt;
      hasExt = fn.test(GEL_IMAGE) || hasExt;
    }

    boolean hasAssociatedSequences =
            er.extractNullAwareOptValue(DwcTerm.associatedSequences).isPresent();

    br.setIsSequenced(hasExt || hasAssociatedSequences);
  }

  /** {@link DwcTerm#associatedSequences} interpretation. */
  public static void interpretAssociatedSequences(ExtendedRecord er, BasicRecord br) {
    List<String> list = er.extractListValue(DEFAULT_SEPARATOR + "|;", DwcTerm.associatedSequences);
    if (!list.isEmpty()) {
      br.setAssociatedSequences(list);
    }
  }

//  /** Sets the coreId field. */
//  public static void setCoreId(ExtendedRecord er, BasicRecord br) {
//    Optional.ofNullable(er.getCoreId()).ifPresent(br::setCoreId);
//  }
}
