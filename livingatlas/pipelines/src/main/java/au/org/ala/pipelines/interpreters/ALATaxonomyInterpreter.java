package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import com.google.common.base.Enums;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ALAMatchIssueType;
import org.gbif.pipelines.io.avro.ALAMatchType;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.NameType;

/** Providing taxonomic matching functionality for occurrence records. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALATaxonomyInterpreter {

  private static final Map<ALAMatchIssueType, InterpretationRemark> ISSUE_MAP =
      Collections.unmodifiableMap(
          Arrays.stream(
                  new Object[][] {
                    new Object[] {
                      ALAMatchIssueType.affinitySpecies, ALAOccurrenceIssue.TAXON_AFFINITY_SPECIES
                    },
                    new Object[] {
                      ALAMatchIssueType.associatedNameExcluded,
                      ALAOccurrenceIssue.TAXON_EXCLUDED_ASSOCIATED
                    },
                    new Object[] {
                      ALAMatchIssueType.conferSpecies, ALAOccurrenceIssue.TAXON_CONFER_SPECIES
                    },
                    new Object[] {
                      ALAMatchIssueType.excludedSpecies, ALAOccurrenceIssue.TAXON_EXCLUDED
                    },
                    new Object[] {ALAMatchIssueType.genericError, ALAOccurrenceIssue.TAXON_ERROR},
                    new Object[] {
                      ALAMatchIssueType.hintMismatch, ALAOccurrenceIssue.TAXON_SCOPE_MISMATCH
                    },
                    new Object[] {ALAMatchIssueType.homonym, ALAOccurrenceIssue.TAXON_HOMONYM},
                    new Object[] {
                      ALAMatchIssueType.indeterminateSpecies,
                      ALAOccurrenceIssue.TAXON_INDETERMINATE_SPECIES
                    },
                    new Object[] {
                      ALAMatchIssueType.matchedToMisappliedName,
                      ALAOccurrenceIssue.TAXON_MISAPPLIED_MATCHED
                    },
                    new Object[] {
                      ALAMatchIssueType.misappliedName, ALAOccurrenceIssue.TAXON_MISAPPLIED
                    },
                    // Not needed but implicitly present
                    // new Object[] { ALAMatchIssueType.noIssue, null },
                    new Object[] {ALAMatchIssueType.noMatch, OccurrenceIssue.TAXON_MATCH_NONE},
                    new Object[] {
                      ALAMatchIssueType.parentChildSynonym,
                      ALAOccurrenceIssue.TAXON_PARENT_CHILD_SYNONYM
                    },
                    new Object[] {
                      ALAMatchIssueType.questionSpecies, ALAOccurrenceIssue.TAXON_QUESTION_SPECIES
                    },
                    new Object[] {
                      ALAMatchIssueType.speciesPlural, ALAOccurrenceIssue.TAXON_SPECIES_PLURAL
                    }
                  })
              .collect(
                  Collectors.toMap(
                      p -> (ALAMatchIssueType) p[0], p -> (InterpretationRemark) p[1])));

  /**
   * Perform quality checks on the incoming record.
   *
   * @param dataResource The associated data resource.
   * @return A quality check function
   */
  public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaSourceQualityChecks(
      final ALACollectoryMetadata dataResource, final KeyValueStore<String, Boolean> kingdomStore) {
    final Map<String, String> defaults =
        dataResource == null ? null : dataResource.getDefaultDarwinCoreValues();

    return (er, atr) -> {
      String taxonRank = extractValue(er, DwcTerm.taxonRank, defaults);
      String scientificName = extractValue(er, DwcTerm.scientificName, defaults);
      String vernacularName = extractValue(er, DwcTerm.vernacularName, defaults);
      String kingdom = extractValue(er, DwcTerm.kingdom, defaults);

      if (taxonRank == null) {
        addIssue(atr, ALAOccurrenceIssue.MISSING_TAXONRANK);
      }
      if (scientificName == null && vernacularName == null) {
        addIssue(atr, ALAOccurrenceIssue.NAME_NOT_SUPPLIED);
      }
      if (kingdom != null && kingdomStore != null) {
        Boolean check = kingdomStore.get(kingdom);
        if (check != null && !check) {
          addIssue(atr, ALAOccurrenceIssue.UNKNOWN_KINGDOM);
        }
      }
    };
  }

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   *
   * @param dataResource The associated data resource for the record, for defaults and hints
   * @param kvStore The taxonomic search lookup
   * @return The interpretation function
   */
  public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaTaxonomyInterpreter(
      final ALACollectoryMetadata dataResource,
      final KeyValueStore<NameSearch, NameUsageMatch> kvStore,
      final Boolean matchOnTaxonID) {
    final Map<String, List<String>> hints = dataResource == null ? null : dataResource.getHintMap();
    final Map<String, String> defaults =
        dataResource == null ? null : dataResource.getDefaultDarwinCoreValues();

    return (er, atr) -> {
      atr.setId(er.getId());

      if (kvStore != null) {
        String genus = extractValue(er, DwcTerm.genus, defaults);
        if (genus == null) {
          genus = extractValue(er, DwcTerm.genericName, defaults);
        }
        NameSearch.NameSearchBuilder builder = NameSearch.builder();
        if (matchOnTaxonID) {
          builder.taxonID(extractValue(er, DwcTerm.taxonID, defaults));
        }
        NameSearch matchRequest =
            builder
                .kingdom(extractValue(er, DwcTerm.kingdom, defaults))
                .phylum(extractValue(er, DwcTerm.phylum, defaults))
                .clazz(extractValue(er, DwcTerm.class_, defaults))
                .order(extractValue(er, DwcTerm.order, defaults))
                .family(extractValue(er, DwcTerm.family, defaults))
                .genus(genus)
                .scientificName(extractValue(er, DwcTerm.scientificName, defaults))
                .rank(extractValue(er, DwcTerm.taxonRank, defaults))
                .verbatimTaxonRank(extractValue(er, DwcTerm.verbatimTaxonRank, defaults))
                .specificEpithet(extractValue(er, DwcTerm.specificEpithet, defaults))
                .infraspecificEpithet(extractValue(er, DwcTerm.infraspecificEpithet, defaults))
                .scientificNameAuthorship(
                    extractValue(er, DwcTerm.scientificNameAuthorship, defaults))
                .vernacularName(extractValue(er, DwcTerm.vernacularName, defaults))
                .hints(hints)
                .build();

        NameUsageMatch usageMatch = kvStore.get(matchRequest);
        if (isEmpty(usageMatch)) {
          // happens when we get an empty response from the WS
          addIssue(atr, OccurrenceIssue.TAXON_MATCH_NONE);
        } else {
          // Do a straight property-property copy to catch complications with field names
          atr.setTaxonConceptID(usageMatch.getTaxonConceptID());
          atr.setScientificName(usageMatch.getScientificName());
          atr.setScientificNameAuthorship(usageMatch.getScientificNameAuthorship());
          atr.setTaxonRank(usageMatch.getRank());
          atr.setTaxonRankID(usageMatch.getRankID());
          atr.setLft(usageMatch.getLft());
          atr.setRgt(usageMatch.getRgt());
          atr.setMatchType(usageMatch.getMatchType());
          atr.setNameType(usageMatch.getNameType());
          // Ignore synonym type
          atr.setKingdom(usageMatch.getKingdom());
          atr.setKingdomID(usageMatch.getKingdomID());
          atr.setPhylum(usageMatch.getPhylum());
          atr.setPhylumID(usageMatch.getPhylumID());
          atr.setClasss(usageMatch.getClasss());
          atr.setClassID(usageMatch.getClassID());
          atr.setOrder(usageMatch.getOrder());
          atr.setOrderID(usageMatch.getOrderID());
          atr.setFamily(usageMatch.getFamily());
          atr.setFamilyID(usageMatch.getFamilyID());
          atr.setGenus(usageMatch.getGenus());
          atr.setGenusID(usageMatch.getGenusID());
          atr.setSpecies(usageMatch.getSpecies());
          atr.setSpeciesID(usageMatch.getSpeciesID());
          atr.setVernacularName(usageMatch.getVernacularName());
          atr.setSpeciesGroup(
              usageMatch.getSpeciesGroup() == null
                  ? new ArrayList<>()
                  : usageMatch.getSpeciesGroup());
          atr.setSpeciesSubgroup(
              usageMatch.getSpeciesSubgroup() == null
                  ? new ArrayList<>()
                  : usageMatch.getSpeciesSubgroup());
          // Issues can happen for match/nomatch
        }
        if (usageMatch != null && usageMatch.getIssues() != null) {
          // Translate issues returned by the namematching service
          for (String issue : usageMatch.getIssues()) {
            ALAMatchIssueType type =
                Enums.getIfPresent(ALAMatchIssueType.class, issue)
                    .or(ALAMatchIssueType.genericError);
            InterpretationRemark oi = ISSUE_MAP.get(type);
            if (oi != null) {
              addIssue(atr, oi);
            }
          }
        }
      }
    };
  }

  /**
   * Perform quality checks on the outgoing record.
   *
   * @param dataResource The associated data resource.
   * @return The quality check function
   */
  public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaResultQualityChecks(
      final ALACollectoryMetadata dataResource) {
    final Map<String, String> defaults =
        dataResource == null ? null : dataResource.getDefaultDarwinCoreValues();

    return (er, atr) -> {
      String rank = atr.getTaxonRank();
      String scientificName = atr.getScientificName();

      // Check to see whether the resulting taxon matches a supplied default name
      if (defaults != null && rank != null && scientificName != null) {
        String defaultName = defaults.get(rank.toLowerCase());
        if (scientificName.equalsIgnoreCase(defaultName)) {
          addIssue(atr, ALAOccurrenceIssue.TAXON_DEFAULT_MATCH);
        }
      }

      // Additional issue flags for matches
      if (atr.getMatchType() != null) {
        ALAMatchType matchType =
            Enums.getIfPresent(ALAMatchType.class, atr.getMatchType()).orNull();
        if (matchType == ALAMatchType.fuzzyMatch) addIssue(atr, OccurrenceIssue.TAXON_MATCH_FUZZY);
        if (matchType == ALAMatchType.higherMatch)
          addIssue(atr, OccurrenceIssue.TAXON_MATCH_HIGHERRANK);
      }

      // Additional issue flags for names
      if (atr.getNameType() != null) {
        NameType nameType = Enums.getIfPresent(NameType.class, atr.getNameType()).orNull();
        if (nameType == NameType.NO_NAME
            || nameType == NameType.INFORMAL
            || nameType == NameType.PLACEHOLDER) {
          addIssue(atr, ALAOccurrenceIssue.INVALID_SCIENTIFIC_NAME);
        }
      }
    };
  }

  /**
   * Add an issue to the issues list.
   *
   * @param atr The record
   * @param issue The issue
   */
  protected static void addIssue(ALATaxonRecord atr, InterpretationRemark issue) {
    ModelUtils.addIssue(atr, issue.getId());
  }

  protected static boolean isEmpty(NameUsageMatch response) {
    return response == null || !response.isSuccess();
  }

  /**
   * Extract a value from a record, with a potential default value
   *
   * @param er The extened record
   * @param term The term to look up
   * @param defaults Any defaults that apply to this value
   * @return The resulting value, or null for not found
   */
  protected static String extractValue(ExtendedRecord er, Term term, Map<String, String> defaults) {
    String value = ModelUtils.extractValue(er, term);
    if (value == null && defaults != null && !defaults.isEmpty()) {
      value = defaults.get(term.simpleName());
    }
    return value;
  }
}
