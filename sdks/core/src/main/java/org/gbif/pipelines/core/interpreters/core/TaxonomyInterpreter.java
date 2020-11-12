package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.nameparser.NameParserGBIF;
import org.gbif.nameparser.api.NameParser;
import org.gbif.nameparser.api.UnparsableNameException;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.Authorship;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.NamePart;
import org.gbif.pipelines.io.avro.NameType;
import org.gbif.pipelines.io.avro.NomCode;
import org.gbif.pipelines.io.avro.ParsedName;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.State;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatch;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an
 * existing specie.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonomyInterpreter {

  private static final RankedName INCERTAE_SEDIS =
      RankedName.newBuilder()
          .setRank(Rank.KINGDOM)
          .setName(Kingdom.INCERTAE_SEDIS.scientificName())
          .setKey(Kingdom.INCERTAE_SEDIS.nubUsageKey())
          .build();
  private static final NameParser NAME_PARSER = new NameParserGBIF();

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, TaxonRecord> taxonomyInterpreter(
      KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
    return (er, tr) -> {
      if (kvStore == null) {
        return;
      }

      ModelUtils.checkNullOrEmpty(er);

      SpeciesMatchRequest matchRequest =
          SpeciesMatchRequest.builder()
              .withKingdom(extractValue(er, DwcTerm.kingdom))
              .withPhylum(extractValue(er, DwcTerm.phylum))
              .withClazz(extractValue(er, DwcTerm.class_))
              .withOrder(extractValue(er, DwcTerm.order))
              .withFamily(extractValue(er, DwcTerm.family))
              .withGenus(extractValue(er, DwcTerm.genus))
              .withScientificName(extractValue(er, DwcTerm.scientificName))
              .withRank(extractValue(er, DwcTerm.taxonRank))
              .withVerbatimRank(extractValue(er, DwcTerm.verbatimTaxonRank))
              .withSpecificEpithet(extractValue(er, DwcTerm.specificEpithet))
              .withInfraspecificEpithet(extractValue(er, DwcTerm.infraspecificEpithet))
              .withScientificNameAuthorship(extractValue(er, DwcTerm.scientificNameAuthorship))
              .withGenericName(extractValue(er, GbifTerm.genericName))
              .build();

      NameUsageMatch usageMatch = null;
      try {
        usageMatch = kvStore.get(matchRequest);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
      }

      if (usageMatch == null || isEmpty(usageMatch) || checkFuzzy(usageMatch, matchRequest)) {
        // "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        addIssue(tr, TAXON_MATCH_NONE);
        tr.setUsage(INCERTAE_SEDIS);
        tr.setClassification(Collections.singletonList(INCERTAE_SEDIS));
      } else {

        MatchType matchType = usageMatch.getDiagnostics().getMatchType();

        if (MatchType.NONE == matchType) {
          addIssue(tr, TAXON_MATCH_NONE);
        } else if (MatchType.FUZZY == matchType) {
          addIssue(tr, TAXON_MATCH_FUZZY);
        } else if (MatchType.HIGHERRANK == matchType) {
          addIssue(tr, TAXON_MATCH_HIGHERRANK);
        }

        // parse name into pieces - we don't get them from the nub lookup
        try {
          if (Objects.nonNull(usageMatch.getUsage())) {
            org.gbif.nameparser.api.ParsedName pn =
                NAME_PARSER.parse(
                    usageMatch.getUsage().getName(),
                    org.gbif.nameparser.api.Rank.valueOf(usageMatch.getUsage().getRank().name()));
            tr.setUsageParsedName(toParsedNameAvro(pn));
          }
        } catch (UnparsableNameException e) {
          if (e.getType().isParsable()) {
            log.warn(
                "Fail to parse backbone {} name for occurrence {}: {}",
                e.getType(),
                er.getId(),
                e.getName());
          }
        }
        // convert taxon record
        TaxonRecordConverter.convert(usageMatch, tr);
      }

      tr.setId(er.getId());
    };
  }

  /**
   * To be able to return NONE, if response is FUZZY and higher taxa is null or empty Fix for
   * https://github.com/gbif/pipelines/issues/254
   */
  @VisibleForTesting
  protected static boolean checkFuzzy(NameUsageMatch usageMatch, SpeciesMatchRequest matchRequest) {
    boolean isFuzzy = MatchType.FUZZY == usageMatch.getDiagnostics().getMatchType();
    boolean isEmptyTaxa =
        Strings.isNullOrEmpty(matchRequest.getKingdom())
            && Strings.isNullOrEmpty(matchRequest.getPhylum())
            && Strings.isNullOrEmpty(matchRequest.getClazz())
            && Strings.isNullOrEmpty(matchRequest.getOrder())
            && Strings.isNullOrEmpty(matchRequest.getFamily());
    return isFuzzy && isEmptyTaxa;
  }

  /**
   * Converts a {@link org.gbif.nameparser.api.ParsedName} into {@link
   * org.gbif.pipelines.io.avro.ParsedName}.
   */
  private static ParsedName toParsedNameAvro(org.gbif.nameparser.api.ParsedName pn) {
    ParsedName.Builder builder =
        ParsedName.newBuilder()
            .setAbbreviated(pn.isAbbreviated())
            .setAutonym(pn.isAutonym())
            .setBinomial(pn.isBinomial())
            .setCandidatus(pn.isCandidatus())
            .setCultivarEpithet(pn.getCultivarEpithet())
            .setDoubtful(pn.isDoubtful())
            .setGenus(pn.getGenus())
            .setWarnings(pn.getWarnings())
            .setUninomial(pn.getUninomial())
            .setUnparsed(pn.getUnparsed())
            .setTrinomial(pn.isTrinomial())
            .setIncomplete(pn.isIncomplete())
            .setIndetermined(pn.isIndetermined())
            .setTerminalEpithet(pn.getTerminalEpithet())
            .setInfragenericEpithet(pn.getInfragenericEpithet())
            .setInfraspecificEpithet(pn.getInfraspecificEpithet())
            .setNomenclaturalNotes(pn.getNomenclaturalNotes())
            .setTaxonomicNote(pn.getTaxonomicNote())
            .setStrain(pn.getStrain())
            .setSanctioningAuthor(pn.getSanctioningAuthor())
            .setRemarks(pn.getRemarks())
            .setSpecificEpithet(pn.getSpecificEpithet());

    // Nullable fields
    Optional.ofNullable(pn.getBasionymAuthorship())
        .ifPresent(authorship -> builder.setBasionymAuthorship(toAuthorshipAvro(authorship)));
    Optional.ofNullable(pn.getCombinationAuthorship())
        .ifPresent(authorship -> builder.setCombinationAuthorship(toAuthorshipAvro(authorship)));
    Optional.ofNullable(pn.getCode())
        .ifPresent(code -> builder.setCode(NomCode.valueOf(code.name())));
    Optional.ofNullable(pn.getType())
        .ifPresent(type -> builder.setType(NameType.valueOf(type.name())));
    Optional.ofNullable(pn.getNotho())
        .ifPresent(notho -> builder.setNotho(NamePart.valueOf(notho.name())));
    Optional.ofNullable(pn.getRank()).ifPresent(rank -> builder.setRank(Rank.valueOf(rank.name())));
    Optional.ofNullable(pn.getState())
        .ifPresent(state -> builder.setState(State.valueOf(state.name())));
    return builder.build();
  }

  /**
   * Converts a {@link org.gbif.nameparser.api.Authorship} into {@link
   * org.gbif.pipelines.io.avro.Authorship}.
   */
  private static Authorship toAuthorshipAvro(org.gbif.nameparser.api.Authorship authorship) {
    return Authorship.newBuilder()
        .setEmpty(authorship.isEmpty())
        .setYear(authorship.getYear())
        .setAuthors(authorship.getAuthors())
        .setExAuthors(authorship.getExAuthors())
        .build();
  }

  private static boolean isEmpty(NameUsageMatch response) {
    return response == null
        || response.getUsage() == null
        || (response.getClassification() == null || response.getClassification().isEmpty())
        || response.getDiagnostics() == null;
  }
}
