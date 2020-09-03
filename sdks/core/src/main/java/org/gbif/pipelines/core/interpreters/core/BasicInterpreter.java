package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS;
import static org.gbif.api.vocabulary.OccurrenceIssue.INDIVIDUAL_COUNT_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT;
import static org.gbif.api.vocabulary.OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE;
import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.TYPE_STATUS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.identifier.AgentIdentifierParser;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BasicInterpreter {

  public static final String GBIF_ID_INVALID = "GBIF_ID_INVALID";

  private static final Parsable<String> TYPE_NAME_PARSER =
      org.gbif.common.parsers.TypifiedNameParser.getInstance();

  /** Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretGbifId(
      HBaseLockingKeyService keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn) {
    gbifIdFn = gbifIdFn == null ? interpretCopyGbifId() : gbifIdFn;
    return useExtendedRecordId
        ? gbifIdFn
        : interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid);
  }

  /** Generates or gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretGbifId(
      HBaseLockingKeyService keygenService, boolean isTripletValid, boolean isOccurrenceIdValid) {
    return (er, br) -> {
      if (keygenService == null) {
        return;
      }

      Set<String> uniqueStrings = new HashSet<>(2);

      // Adds occurrenceId
      if (isOccurrenceIdValid) {
        String occurrenceId = extractValue(er, DwcTerm.occurrenceID);
        if (!Strings.isNullOrEmpty(occurrenceId)) {
          uniqueStrings.add(occurrenceId);
        }
      }

      // Adds triplet
      if (isTripletValid) {
        String ic = extractValue(er, DwcTerm.institutionCode);
        String cc = extractValue(er, DwcTerm.collectionCode);
        String cn = extractValue(er, DwcTerm.catalogNumber);
        OccurrenceKeyBuilder.buildKey(ic, cc, cn).ifPresent(uniqueStrings::add);
      }

      if (!uniqueStrings.isEmpty()) {
        try {
          KeyLookupResult key =
              Optional.ofNullable(keygenService.findKey(uniqueStrings))
                  .orElse(keygenService.generateKey(uniqueStrings));

          br.setGbifId(key.getKey());
        } catch (IllegalStateException ex) {
          log.warn(ex.getMessage());
          addIssue(br, GBIF_ID_INVALID);
        }
      } else {
        addIssue(br, GBIF_ID_INVALID);
      }
    };
  }

  /** Copies GBIF id from ExtendedRecord id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretCopyGbifId() {
    return (er, br) -> {
      if (StringUtils.isNumeric(er.getId())) {
        br.setGbifId(Long.parseLong(er.getId()));
      }
    };
  }

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
            br.setTypeStatus(parseResult.getPayload().name());
          } else {
            addIssue(br, TYPE_STATUS_INVALID);
          }
          return br;
        };

    VocabularyParser.typeStatusParser().map(er, fn);
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

    VocabularyParser.lifeStageParser().map(er, fn);
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

    VocabularyParser.establishmentMeansParser().map(er, fn);
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
            br.setBasisOfRecord(BasisOfRecord.UNKNOWN.name());
            addIssue(br, BASIS_OF_RECORD_INVALID);
          }
          return br;
        };

    VocabularyParser.basisOfRecordParser().map(er, fn);

    if (br.getBasisOfRecord() == null || br.getBasisOfRecord().isEmpty()) {
      br.setBasisOfRecord(BasisOfRecord.UNKNOWN.name());
      addIssue(br, BASIS_OF_RECORD_INVALID);
    }
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

  /** {@link DwcTerm#sampleSizeValue} interpretation. */
  public static void interpretSampleSizeValue(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.sampleSizeValue)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(br::setSampleSizeValue);
  }

  /** {@link DwcTerm#sampleSizeUnit} interpretation. */
  public static void interpretSampleSizeUnit(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.sampleSizeUnit).map(String::trim).ifPresent(br::setSampleSizeUnit);
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

  /** {@link DcTerm#license} interpretation. */
  public static void interpretLicense(ExtendedRecord er, BasicRecord br) {
    String license =
        extractOptValue(er, DcTerm.license)
            .map(BasicInterpreter::getLicense)
            .map(License::name)
            .orElse(License.UNSPECIFIED.name());

    br.setLicense(license);
  }

  /** {@link GbifTerm#identifiedByID}. */
  public static void interpretIdentifiedByIds(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, GbifTerm.identifiedByID)
        .filter(x -> !x.isEmpty())
        .map(AgentIdentifierParser::parse)
        .map(ArrayList::new)
        .ifPresent(br::setIdentifiedByIds);
  }

  /** {@link GbifTerm#recordedByID} interpretation. */
  public static void interpretRecordedByIds(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, GbifTerm.recordedByID)
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
        if (isOccNull) {
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

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }
}
