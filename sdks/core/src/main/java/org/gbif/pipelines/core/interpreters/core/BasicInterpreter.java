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
import static org.gbif.api.vocabulary.OccurrenceIssue.TYPE_STATUS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.DEFAULT_SEPARATOR;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractListValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

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
import lombok.var;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.identifier.AgentIdentifierParser;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BasicInterpreter {

  private static final Parsable<String> TYPE_NAME_PARSER =
      org.gbif.common.parsers.TypifiedNameParser.getInstance();

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

    SimpleTypeParser.parsePositiveInt(er, DwcTerm.individualCount, fn);
  }

  /** {@link DwcTerm#typeStatus} interpretation. */
  public static void interpretTypeStatus(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<TypeStatus>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            if (br.getTypeStatus() == null) {
              br.setTypeStatus(new ArrayList<>());
            }

            String result = parseResult.getPayload().name();
            if (!br.getTypeStatus().contains(result)) {
              br.getTypeStatus().add(result);
            }
          } else {
            addIssue(br, TYPE_STATUS_INVALID);
          }
          return br;
        };

    VocabularyParser.typeStatusParser().mapList(er, fn);
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

    VocabularyParser.sexParser().map(er, fn);
  }

  /** {@link DwcTerm#basisOfRecord} interpretation. */
  public static void interpretBasisOfRecord(ExtendedRecord er, BasicRecord br) {

    Function<ParseResult<BasisOfRecord>, BasicRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            br.setBasisOfRecord(parseResult.getPayload().name());
          } else {
            br.setBasisOfRecord(BasisOfRecord.OCCURRENCE.name());
            addIssue(br, BASIS_OF_RECORD_INVALID);
          }
          return br;
        };

    VocabularyParser.basisOfRecordParser().map(er, fn);

    if (br.getBasisOfRecord() == null || br.getBasisOfRecord().isEmpty()) {
      br.setBasisOfRecord(BasisOfRecord.OCCURRENCE.name());
      addIssue(br, BASIS_OF_RECORD_INVALID);
    }
  }

  /** {@link GbifTerm#typifiedName} interpretation. */
  public static void interpretTypifiedName(ExtendedRecord er, BasicRecord br) {
    Optional<String> typifiedName = extractOptValue(er, GbifTerm.typifiedName);
    if (typifiedName.isPresent()) {
      br.setTypifiedName(typifiedName.get());
    } else {
      Optional.ofNullable(er.getCoreTerms().get(DwcTerm.typeStatus.qualifiedName()))
          .ifPresent(
              typeStatusValue -> {
                ParseResult<String> result =
                    TYPE_NAME_PARSER.parse(
                        er.getCoreTerms().get(DwcTerm.typeStatus.qualifiedName()));
                if (result.isSuccessful()) {
                  br.setTypifiedName(result.getPayload());
                }
              });
    }
  }

  /** {@link DwcTerm#organismQuantity} interpretation. */
  public static void interpretOrganismQuantity(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.organismQuantity)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(br::setOrganismQuantity);
  }

  /** {@link DwcTerm#organismQuantityType} interpretation. */
  public static void interpretOrganismQuantityType(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.organismQuantityType)
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
    extractOptValue(er, DwcTerm.identifiedByID)
        .filter(x -> !x.isEmpty())
        .map(AgentIdentifierParser::parse)
        .map(ArrayList::new)
        .ifPresent(br::setIdentifiedByIds);
  }

  /** {@link DwcTerm#recordedByID} interpretation. */
  public static void interpretRecordedByIds(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.recordedByID)
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

      String rawCount = ModelUtils.extractNullAwareValue(er, DwcTerm.individualCount);
      Integer parsedCount = SimpleTypeParser.parsePositiveIntOpt(rawCount).orElse(null);

      String rawOccStatus = ModelUtils.extractNullAwareValue(er, DwcTerm.occurrenceStatus);
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
          Optional.ofNullable(br.getBasisOfRecord())
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
          addIssue(br, OCCURRENCE_STATUS_UNPARSABLE);
        }
      } else if (isCountRubbish) {
        if (isOccNull || isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          addIssue(br, OCCURRENCE_STATUS_UNPARSABLE);
        }
        addIssue(br, INDIVIDUAL_COUNT_INVALID);
      } else if (isCountZero) {
        if (isOccNull && isSpecimen) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          addIssue(br, OCCURRENCE_STATUS_INFERRED_FROM_BASIS_OF_RECORD);
        } else if (isOccNull) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          addIssue(br, OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        } else if (isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          addIssue(br, INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS);
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          addIssue(br, OCCURRENCE_STATUS_UNPARSABLE);
          addIssue(br, OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        }
      } else if (isCountGreaterZero) {
        if (isOccNull) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          addIssue(br, OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        } else if (isOccPresent) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
        } else if (isOccAbsent) {
          br.setOccurrenceStatus(OccurrenceStatus.ABSENT.name());
          addIssue(br, INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS);
        } else if (isOccRubbish) {
          br.setOccurrenceStatus(OccurrenceStatus.PRESENT.name());
          addIssue(br, OCCURRENCE_STATUS_UNPARSABLE);
          addIssue(br, OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT);
        }
      }
    };
  }

  /** {@link DwcTerm#otherCatalogNumbers} interpretation. */
  public static void interpretOtherCatalogNumbers(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(DEFAULT_SEPARATOR + "|;", er, DwcTerm.otherCatalogNumbers);
    if (!list.isEmpty()) {
      br.setOtherCatalogNumbers(list);
    }
  }

  /** {@link DwcTerm#recordedBy} interpretation. */
  public static void interpretRecordedBy(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(er, DwcTerm.recordedBy);
    if (!list.isEmpty()) {
      br.setRecordedBy(list);
    }
  }

  /** {@link DwcTerm#identifiedBy} interpretation. */
  public static void interpretIdentifiedBy(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(er, DwcTerm.identifiedBy);
    if (!list.isEmpty()) {
      br.setIdentifiedBy(list);
    }
  }

  /** {@link DwcTerm#preparations} interpretation. */
  public static void interpretPreparations(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(er, DwcTerm.preparations);
    if (!list.isEmpty()) {
      br.setPreparations(list);
    }
  }

  /** {@link org.gbif.dwc.terms.GbifTerm#projectId} interpretation. */
  public static void interpretProjectId(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(er, GbifTerm.projectId);
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
        extractNullAwareOptValue(er, DwcTerm.associatedSequences).isPresent();

    br.setIsSequenced(hasExt || hasAssociatedSequences);
  }

  /** {@link DwcTerm#associatedSequences} interpretation. */
  public static void interpretAssociatedSequences(ExtendedRecord er, BasicRecord br) {
    List<String> list = extractListValue(DEFAULT_SEPARATOR + "|;", er, DwcTerm.associatedSequences);
    if (!list.isEmpty()) {
      br.setAssociatedSequences(list);
    }
  }

  /** Sets the coreId field. */
  public static void setCoreId(ExtendedRecord er, BasicRecord br) {
    Optional.ofNullable(er.getCoreId()).ifPresent(br::setCoreId);
  }
}
