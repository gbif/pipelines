package org.gbif.pipelines.http.match2;

import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import static org.gbif.pipelines.interpretation.parsers.VocabularyParsers.rankParser;
import static org.gbif.pipelines.interpretation.parsers.VocabularyParsers.verbatimTaxonRankParser;

/**
 * Converter to create queries for the name match service.
 */
public class NameUsageMatchQueryConverter {

  private NameUsageMatchQueryConverter() {}

  private static class AtomizedFields {

    private final String genus;
    private final String specificEpithet;
    private final String infraspecificEpithet;

    private static Builder newBuilder() {
      return new Builder();
    }

    private AtomizedFields(Builder builder) {
      genus = builder.genus;
      specificEpithet = builder.specificEpithet;
      infraspecificEpithet = builder.infraspecificEpithet;
    }

    public String getGenus() {
      return genus;
    }

    public String getSpecificEpithet() {
      return specificEpithet;
    }

    public String getInfraspecificEpithet() {
      return infraspecificEpithet;
    }

    public static final class Builder {

      private String genus;
      private String specificEpithet;
      private String infraspecificEpithet;

      private Builder() {}

      public Builder withGenus(String genus) {
        this.genus = Strings.emptyToNull(genus);
        return this;
      }

      public Builder withSpecificEpithet(String specificEpithet) {
        this.specificEpithet = Strings.emptyToNull(specificEpithet);
        return this;
      }

      public Builder withInfraspecificEpithet(String infraspecificEpithet) {
        this.infraspecificEpithet = Strings.emptyToNull(infraspecificEpithet);
        return this;
      }

      public AtomizedFields build() {
        return new AtomizedFields(this);
      }
    }
  }

  /**
   * Converts a {@link Map} of terms to {@link Map} with the params needed to call the {@link SpeciesMatch2Service}.
   */
  public static Map<String, String> convert(final Map<String, String> terms) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    AtomizedFields.Builder atomizedFieldsBuilder = AtomizedFields.newBuilder();

    getTaxonValue(terms, DwcTerm.kingdom).ifPresent(cleanValue -> builder.put("kingdom", cleanValue));
    getTaxonValue(terms, DwcTerm.phylum).ifPresent(cleanValue -> builder.put("phylum", cleanValue));
    getTaxonValue(terms, DwcTerm.class_).ifPresent(cleanValue -> builder.put("class", cleanValue));
    getTaxonValue(terms, DwcTerm.order).ifPresent(cleanValue -> builder.put("order", cleanValue));
    getTaxonValue(terms, DwcTerm.family).ifPresent(cleanValue -> builder.put("family", cleanValue));
    getTaxonValue(terms, DwcTerm.specificEpithet).ifPresent(atomizedFieldsBuilder::withSpecificEpithet);
    getTaxonValue(terms, DwcTerm.infraspecificEpithet).ifPresent(atomizedFieldsBuilder::withInfraspecificEpithet);
    getTaxonValue(terms, DwcTerm.genus).ifPresent(cleanValue -> {
      builder.put("genus", cleanValue);
      atomizedFieldsBuilder.withGenus(cleanValue);
    });

    String genericName = getTaxonValue(terms, GbifTerm.genericName).orElse(null);
    String scientificNameAuthorship = getAuthorValue(terms, DwcTerm.scientificNameAuthorship).orElse(null);

    AtomizedFields atomizedFields = atomizedFieldsBuilder.build();

    interpretRank(terms, atomizedFields).ifPresent(rank -> builder.put("rank", rank.name()));

    interpretScientificName(terms, atomizedFields, scientificNameAuthorship, genericName)
      .ifPresent(scientificName -> builder.put("name", scientificName));

    builder.put("strict", Boolean.FALSE.toString());
    builder.put("verbose", Boolean.FALSE.toString());
    return builder.build();
  }

  /**
   * Gets a clean version of taxa parameter.
   */
  private static Optional<String> getTaxonValue(Map<String, String> terms, Term term) {
    return Optional.ofNullable(terms.get(term.qualifiedName())).map(ClassificationUtils::clean);
  }

  /**
   * Gets a clean version of field with authorship.
   */
  private static Optional<String> getAuthorValue(final Map<String, String> terms, Term term) {
    return Optional.ofNullable(terms.get(term.qualifiedName())).map(ClassificationUtils::cleanAuthor);
  }

  private static Optional<Rank> interpretRank(
    final Map<String, String> terms, AtomizedFields atomizedFields
  ) {
    Optional<Rank> interpretedRank = rankParser().map(terms, Function.identity())
      .map(rankParseResult -> rankParseResult.isSuccessful()
        ? rankParseResult.getPayload()
        : verbatimTaxonRankParser().map(terms, ParseResult::getPayload).get());

    if (!interpretedRank.isPresent()) {
      return fromAtomizedFields(atomizedFields);
    }

    return interpretedRank;
  }

  private static Optional<Rank> fromAtomizedFields(AtomizedFields atomizedFields) {
    if (Objects.nonNull(atomizedFields.getGenus())) {
      return Optional.empty();
    }
    if (!Objects.nonNull(atomizedFields.getSpecificEpithet())) {
      return Optional.of(Rank.GENUS);
    }
    return Objects.nonNull(atomizedFields.getInfraspecificEpithet())
      ? Optional.of(Rank.INFRASPECIFIC_NAME)
      : Optional.of(Rank.SPECIES);
  }

  /**
   * Assembles the most complete scientific name based on full and individual name parts.
   */
  private static Optional<String> interpretScientificName(
    final Map<String, String> terms, AtomizedFields atomizedFields, String authorship, String genericName
  ) {
    Optional<String> interpretedScientificName =
      Optional.of(terms.get(DwcTerm.scientificName.qualifiedName())).map(scientificNameValue -> {
        String scientificName = ClassificationUtils.clean(scientificNameValue);
        return scientificName != null && !Strings.isNullOrEmpty(authorship) && !scientificName.toLowerCase()
          .contains(authorship.toLowerCase()) ? scientificName + " " + authorship : scientificName;
      });
    if (!interpretedScientificName.isPresent()) {
      // handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
      ParsedName pn = new ParsedName();
      if (Strings.isNullOrEmpty(genericName)) {
        pn.setGenusOrAbove(atomizedFields.getGenus());
      } else {
        pn.setGenusOrAbove(genericName);
      }
      pn.setSpecificEpithet(atomizedFields.getSpecificEpithet());
      pn.setInfraSpecificEpithet(atomizedFields.getInfraspecificEpithet());
      pn.setAuthorship(authorship);
      return Optional.ofNullable(pn.canonicalNameComplete());
    }
    return interpretedScientificName;

  }
}