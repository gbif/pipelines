package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;

import java.util.ArrayList;
import java.util.function.BiConsumer;

import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALATaxonomyInterpreter {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, ALATaxonRecord> alaTaxonomyInterpreter(
      KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvStore) {
    return (er, atr) -> {

      if (kvStore != null) {

        ALASpeciesMatchRequest matchRequest = ALASpeciesMatchRequest.builder()
            .kingdom(extractValue(er, DwcTerm.kingdom))
            .phylum(extractValue(er, DwcTerm.phylum))
            .clazz(extractValue(er, DwcTerm.class_))
            .order(extractValue(er, DwcTerm.order))
            .family(extractValue(er, DwcTerm.family))
            .genus(extractValue(er, DwcTerm.genus))
            .scientificName(extractValue(er, DwcTerm.scientificName))
            .rank(extractValue(er, DwcTerm.taxonRank))
            .verbatimTaxonRank(extractValue(er, DwcTerm.verbatimTaxonRank))
            .specificEpithet(extractValue(er, DwcTerm.specificEpithet))
            .infraspecificEpithet(extractValue(er, DwcTerm.infraspecificEpithet))
            .scientificNameAuthorship(extractValue(er, DwcTerm.scientificNameAuthorship))
            .genericName(extractValue(er, GbifTerm.genericName))
            .build();

        atr.setId(er.getId());

                ALANameUsageMatch usageMatch = kvStore.get(matchRequest);

                if (usageMatch == null || isEmpty(usageMatch)) {
                    // "NO_MATCHING_RESULTS". This
                    // happens when we get an empty response from the WS
                    addIssue(atr, TAXON_MATCH_NONE);
//                    atr.setUsage(INCERTAE_SEDIS);
//                    atr.setClassification(Collections.singletonList(INCERTAE_SEDIS));
                } else {

//                    for (Map.Entry<String, Object> entry : usageMatch.) {

                    try {
                        BeanUtils.copyProperties(atr, usageMatch);
                        //initialise to avoid AVRO serialisation error
                        if(usageMatch.getSpeciesGroup() == null){
                            atr.setSpeciesGroup(new ArrayList<String>());
                        }
                        if(usageMatch.getSpeciesSubgroup() == null){
                            atr.setSpeciesSubgroup(new ArrayList<String>());
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
//                    }

//                    String matchType = usageMatch.getMatchType();
//
//                    if (MatchType.NONE == matchType) {
//                        addIssue(atr, TAXON_MATCH_NONE);
//                    } else if (MatchType.FUZZY == matchType) {
//                        addIssue(atr, TAXON_MATCH_FUZZY);
//                    } else if (MatchType.HIGHERRANK == matchType) {
//                        addIssue(atr, TAXON_MATCH_HIGHERRANK);
//                    }
//
//                    // parse name into pieces - we don't get them from the nub lookup
//                    try {
//                        if (Objects.nonNull(usageMatch.getUsage())) {
//                            org.gbif.nameparser.api.ParsedName pn = NAME_PARSER.parse(usageMatch.getUsage().getName(),
//                                    org.gbif.nameparser.api.Rank.valueOf(usageMatch.getUsage().getRank().name()));
//                            tr.setUsageParsedName(toParsedNameAvro(pn));
//                        }
//                    } catch (UnparsableNameException e) {
//                        if (e.getType().isParsable()) {
//                            log.warn("Fail to parse backbone {} name for occurrence {}: {}", e.getType(), er.getId(), e.getName());
//                        }
//                    }
          // convert taxon record
//                    TaxonRecordConverter.convert(usageMatch, tr);
        }

        atr.setId(er.getId());
      }
    };
  }

    private static boolean isEmpty(ALANameUsageMatch response) {
        return response == ALANameUsageMatch.FAIL;
    }
}
