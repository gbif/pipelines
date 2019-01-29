package org.gbif.pipelines.parsers.ws.client.match2;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import static org.gbif.pipelines.parsers.parsers.VocabularyParsers.rankParser;
import static org.gbif.pipelines.parsers.parsers.VocabularyParsers.verbatimTaxonRankParser;

/** Converter to create queries for the name match service. */
class MatchQueryConverter {

  private MatchQueryConverter() {}

  private static class Fields {

    private String genus;
    private String specificEpithet;
    private String infraspecificEpithet;

    void setGenus(String genus) {
      this.genus = genus;
    }

    void setSpecificEpithet(String specificEpithet) {
      this.specificEpithet = specificEpithet;
    }

    void setInfraspecificEpithet(String infraspecificEpithet) {
      this.infraspecificEpithet = infraspecificEpithet;
    }
  }


  /**
   * Converts a {@link Map} of terms to {@link Map} with the params needed to call the {@link
   * SpeciesMatchv2Service}.
   */
  static Map<String, String> convert(final SpeciesMatchRequest request) {

    ImmutableMap.Builder<String, String> map = ImmutableMap.builder();

    // Interpret common
    Optional.ofNullable(request.getKingdom()).ifPresent(v -> map.put("kingdom", v));
    Optional.ofNullable(request.getPhylum()).ifPresent(v -> map.put("phylum", v));
    Optional.ofNullable(request.getClass_()).ifPresent(v -> map.put("class", v));
    Optional.ofNullable(request.getOrder()).ifPresent(v -> map.put("order", v));
    Optional.ofNullable(request.getFamily()).ifPresent(v -> map.put("family", v));
    Optional.ofNullable(request.getGenus()).ifPresent(v -> map.put("genus", v));

    Optional.ofNullable(request.getRank()).ifPresent(v -> map.put("rank", v.name()));
    Optional.ofNullable(request.getScientificName()).ifPresent(v -> map.put("name", v));

    map.put("strict", Boolean.FALSE.toString());
    map.put("verbose", Boolean.FALSE.toString());
    return map.build();
  }

  /**
   * Converts a {@link Map} of terms to {@link SpeciesMatchRequest}.
   */
  static SpeciesMatchRequest convert(final Map<String, String> terms) {

    SpeciesMatchRequest.Builder builder = new SpeciesMatchRequest.Builder();

    // Interpret common
    getTaxonValue(terms, DwcTerm.kingdom).ifPresent(builder::setKingdom);
    getTaxonValue(terms, DwcTerm.phylum).ifPresent(builder::setPhylum);
    getTaxonValue(terms, DwcTerm.class_).ifPresent(builder::setClass_);
    getTaxonValue(terms, DwcTerm.order).ifPresent(builder::setOrder);
    getTaxonValue(terms, DwcTerm.family).ifPresent(builder::setFamily);
    getTaxonValue(terms, DwcTerm.genus).ifPresent(builder::setGenus);

    Fields fields = new Fields();
    getTaxonValue(terms, DwcTerm.specificEpithet).ifPresent(fields::setSpecificEpithet);
    getTaxonValue(terms, DwcTerm.infraspecificEpithet).ifPresent(fields::setInfraspecificEpithet);
    getTaxonValue(terms, DwcTerm.genus).ifPresent(fields::setGenus);

    // Interpret rank
    Rank interpretRank = interpretRank(terms, fields);
    Optional.ofNullable(interpretRank).ifPresent(builder::setRank);

    // Interpret scientificName
    String scientificName = interpretScientificName(terms, fields);
    Optional.ofNullable(scientificName).ifPresent(builder::setScientificName);


    return builder.build();
  }

  /** Gets a clean version of taxa parameter. */
  private static Optional<String> getTaxonValue(Map<String, String> terms, Term term) {
    return Optional.ofNullable(terms.get(term.qualifiedName())).map(ClassificationUtils::clean);
  }

  private static Rank interpretRank(Map<String, String> terms, Fields fields) {
    return rankParser()
        .map(terms, Function.identity())
        .map(parseResult -> fromParseResult(terms, parseResult))
        .orElseGet(() -> fromFields(fields));
  }

  private static Rank fromParseResult(Map<String, String> terms, ParseResult<Rank> rank) {
    if (rank.isSuccessful()) {
      return rank.getPayload();
    } else {
      return verbatimTaxonRankParser().map(terms, ParseResult::getPayload).orElse(null);
    }
  }

  private static Rank fromFields(Fields fields) {
    if (fields.genus != null) {
      return null;
    }
    if (fields.specificEpithet == null) {
      return Rank.GENUS;
    }
    return fields.infraspecificEpithet != null ? Rank.INFRASPECIFIC_NAME : Rank.SPECIES;
  }

  /** Assembles the most complete scientific name based on full and individual name parts. */
  private static String interpretScientificName(Map<String, String> terms, Fields fields) {
    String genericName = getTaxonValue(terms, GbifTerm.genericName).orElse(null);

    String authorship =
        Optional.ofNullable(terms.get(DwcTerm.scientificNameAuthorship.qualifiedName()))
            .map(ClassificationUtils::cleanAuthor)
            .orElse(null);

    return Optional.ofNullable(terms.get(DwcTerm.scientificName.qualifiedName()))
        .map(scientificName -> fromScientificName(scientificName, authorship))
        .orElseGet(() -> fromGenericName(fields, genericName, authorship));
  }

  private static String fromScientificName(String scientificName, String authorship) {
    String name = ClassificationUtils.clean(scientificName);

    boolean containsAuthorship =
        name != null
            && !Strings.isNullOrEmpty(authorship)
            && !name.toLowerCase().contains(authorship.toLowerCase());

    return containsAuthorship ? name + " " + authorship : name;
  }

  /**
   * Handle case when the scientific name is null and only given as atomized fields: genus &
   * speciesEpitheton
   */
  private static String fromGenericName(Fields fields, String genericName, String authorship) {
    ParsedName pn = new ParsedName();
    pn.setGenusOrAbove(Strings.isNullOrEmpty(genericName) ? fields.genus : genericName);
    pn.setSpecificEpithet(fields.specificEpithet);
    pn.setInfraSpecificEpithet(fields.infraspecificEpithet);
    pn.setAuthorship(authorship);
    return pn.canonicalNameComplete();
  }
}
