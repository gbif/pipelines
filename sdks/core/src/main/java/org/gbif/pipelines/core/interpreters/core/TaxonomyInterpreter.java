package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.core.utils.ModelUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.utils.IdentificationUtils;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an
 * existing species.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonomyInterpreter {

  public static final String KINGDOM_RANK = "KINGDOM";
  public static final String INCERTAE_SEDIS_NAME = "incertae sedis";
  public static final String INCERTAE_SEDIS_KEY = "0";

  public static final RankedNameWithAuthorship INCERTAE_SEDIS_WITH_AUTHORSHIP =
      RankedNameWithAuthorship.newBuilder()
          .setRank(KINGDOM_RANK)
          .setName(INCERTAE_SEDIS_NAME)
          .setKey(INCERTAE_SEDIS_KEY)
          .build();

  public static final RankedName INCERTAE_SEDIS =
      RankedName.newBuilder()
          .setRank(KINGDOM_RANK)
          .setName(INCERTAE_SEDIS_NAME)
          .setKey(INCERTAE_SEDIS_KEY)
          .build();

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, TaxonRecord> taxonomyInterpreter(
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore, String checklistKey) {
    return (er, tr) -> {
      if (kvStore == null) {
        return;
      }

      ModelUtils.checkNullOrEmpty(er);
      NameUsageMatchRequest nameUsageMatchRequest = createNameUsageMatchRequest(er, checklistKey);
      createTaxonRecord(nameUsageMatchRequest, kvStore, tr);
      tr.setId(er.getId());
    };
  }

  protected static NameUsageMatchRequest createNameUsageMatchRequest(
      ExtendedRecord er, String checklistKey) {
    Map<String, String> termsSource = IdentificationUtils.getIdentificationFieldTermsSource(er);
    // https://github.com/gbif/portal-feedback/issues/4231
    String scientificName =
        extractNullAwareOptValue(termsSource, DwcTerm.scientificName)
            .orElse(extractValue(termsSource, DwcTerm.verbatimIdentification));
    return NameUsageMatchRequest.builder()
        .withChecklistKey(checklistKey)
        .withKingdom(extractValue(termsSource, DwcTerm.kingdom))
        .withPhylum(extractValue(termsSource, DwcTerm.phylum))
        .withClazz(extractValue(termsSource, DwcTerm.class_))
        .withOrder(extractValue(termsSource, DwcTerm.order))
        .withSuperfamily(extractValue(termsSource, DwcTerm.superfamily))
        .withFamily(extractValue(termsSource, DwcTerm.family))
        .withSubfamily(extractValue(termsSource, DwcTerm.subfamily))
        .withTribe(extractValue(termsSource, DwcTerm.tribe))
        .withSubtribe(extractValue(termsSource, DwcTerm.subtribe))
        .withGenus(extractValue(termsSource, DwcTerm.genus))
        .withScientificName(scientificName)
        .withScientificNameAuthorship(extractValue(termsSource, DwcTerm.scientificNameAuthorship))
        .withGenericName(extractValue(termsSource, DwcTerm.genericName))
        .withSpecificEpithet(extractValue(termsSource, DwcTerm.specificEpithet))
        .withInfraspecificEpithet(extractValue(termsSource, DwcTerm.infraspecificEpithet))
        .withScientificNameAuthorship(extractValue(termsSource, DwcTerm.scientificNameAuthorship))
        .withTaxonRank(extractValue(termsSource, DwcTerm.taxonRank))
        .withVerbatimTaxonRank(extractValue(termsSource, DwcTerm.verbatimTaxonRank))
        .withScientificNameID(extractValue(termsSource, DwcTerm.scientificNameID))
        .withTaxonID(extractValue(termsSource, DwcTerm.taxonID))
        .withTaxonConceptID(extractValue(termsSource, DwcTerm.taxonConceptID))
        .build();
  }

  protected static void createTaxonRecord(
      NameUsageMatchRequest nameUsageMatchRequest,
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore,
      TaxonRecord tr) {
    matchTaxon(
        nameUsageMatchRequest,
        kvStore,
        tr,
        r -> {
          tr.setUsage(INCERTAE_SEDIS_WITH_AUTHORSHIP);
          tr.setClassification(Collections.singletonList(INCERTAE_SEDIS));
        },
        r -> {
          TaxonRecordConverter.convert(r, tr);
        });
  }

  public static void matchTaxon(
      NameUsageMatchRequest nameUsageMatchRequest,
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore,
      Issues issuesModel,
      Consumer<NameUsageMatchResponse> noMatchConsumer,
      Consumer<NameUsageMatchResponse> matchConsumer) {

    NameUsageMatchResponse usageMatch = null;
    try {
      usageMatch = kvStore.get(nameUsageMatchRequest);
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
    }

    if (usageMatch == null
        || isEmpty(usageMatch)
        || checkFuzzy(usageMatch, nameUsageMatchRequest)) {
      // "NO_MATCHING_RESULTS". This
      // happens when we get an empty response from the WS
      addIssue(issuesModel, TAXON_MATCH_NONE);
      noMatchConsumer.accept(usageMatch);
    } else {

      NameUsageMatchResponse.MatchType matchType = usageMatch.getDiagnostics().getMatchType();

      // copy any issues asserted by the lookup itself
      if (usageMatch.getDiagnostics().getIssues() != null) {
        addIssueSet(
            issuesModel,
            usageMatch.getDiagnostics().getIssues().stream()
                .map(org.gbif.api.vocabulary.OccurrenceIssue::valueOf)
                .collect(Collectors.toSet()));
      }

      if (NameUsageMatchResponse.MatchType.NONE == matchType) {
        addIssue(issuesModel, TAXON_MATCH_NONE);
      } else if (NameUsageMatchResponse.MatchType.VARIANT == matchType) {
        addIssue(issuesModel, TAXON_MATCH_FUZZY);
      } else if (NameUsageMatchResponse.MatchType.HIGHERRANK == matchType) {
        addIssue(issuesModel, TAXON_MATCH_HIGHERRANK);
      }

      matchConsumer.accept(usageMatch);
    }
  }

  /** Sets the coreId field. */
  public static void setCoreId(ExtendedRecord er, TaxonRecord tr) {
    Optional.ofNullable(er.getCoreId()).ifPresent(tr::setCoreId);
  }

  /** Sets the parentEventId field. */
  public static void setParentEventId(ExtendedRecord er, TaxonRecord tr) {
    extractOptValue(er, DwcTerm.parentEventID).ifPresent(tr::setParentId);
  }

  /**
   * To be able to return NONE, if response is FUZZY and higher taxa is null or empty Fix for
   * https://github.com/gbif/pipelines/issues/254
   */
  @VisibleForTesting
  protected static boolean checkFuzzy(
      NameUsageMatchResponse usageMatch, NameUsageMatchRequest identification) {
    boolean isFuzzy =
        NameUsageMatchResponse.MatchType.VARIANT == usageMatch.getDiagnostics().getMatchType();
    boolean isEmptyTaxa =
        Strings.isNullOrEmpty(identification.getKingdom())
            && Strings.isNullOrEmpty(identification.getPhylum())
            && Strings.isNullOrEmpty(identification.getClazz())
            && Strings.isNullOrEmpty(identification.getOrder())
            && Strings.isNullOrEmpty(identification.getFamily());
    return isFuzzy && isEmptyTaxa;
  }

  /**
   * Converts a {@link org.gbif.nameparser.api.ParsedName} into {@link
   * org.gbif.pipelines.io.avro.ParsedName}.
   */
  private static ParsedName toParsedNameAvro(NameUsageMatchResponse.Usage pn) {
    ParsedName.Builder builder =
        ParsedName.newBuilder()
            .setGenus(pn.getGenericName())
            .setInfragenericEpithet(pn.getInfragenericEpithet())
            .setInfraspecificEpithet(pn.getInfraspecificEpithet())
            .setSpecificEpithet(pn.getSpecificEpithet());

    // Nullable fields
    Optional.ofNullable(pn.getCode())
        .ifPresent(code -> builder.setCode(convertToEnum(NomCode.class, code)));
    Optional.ofNullable(pn.getType())
        .ifPresent(type -> builder.setType(convertToEnum(NameType.class, type)));
    Optional.ofNullable(pn.getRank())
        .ifPresent(rank -> builder.setRank(convertToEnum(NameRank.class, rank)));
    return builder.build();
  }

  public static <T extends Enum<T>> T convertToEnum(Class<T> enumClass, String value) {
    try {
      return Enum.valueOf(enumClass, value.toUpperCase());
    } catch (IllegalArgumentException | NullPointerException e) {
      return null; // Return null if conversion fails
    }
  }

  private static boolean isEmpty(NameUsageMatchResponse response) {
    return response == null
        || response.getUsage() == null
        || (response.getClassification() == null || response.getClassification().isEmpty())
        || response.getDiagnostics() == null;
  }
}
