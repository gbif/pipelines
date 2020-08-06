package org.gbif.pipelines.core.parsers;

import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.EstablishmentMeansParser;
import org.gbif.common.parsers.LifeStageParser;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.SexParser;
import org.gbif.common.parsers.TypeStatusParser;
import org.gbif.common.parsers.core.EnumParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Utility class that parses Enum based terms. */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class VocabularyParser<T extends Enum<T>> {

  private static final TypeStatusParser TYPE_PARSER = TypeStatusParser.getInstance();
  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final SexParser SEX_PARSER = SexParser.getInstance();
  private static final EstablishmentMeansParser EST_PARSER = EstablishmentMeansParser.getInstance();
  private static final LifeStageParser LST_PARSER = LifeStageParser.getInstance();
  private static final CountryParser COUNTRY_PARSER = CountryParser.getInstance();
  private static final ContinentParser CONTINENT_PARSER = ContinentParser.getInstance();
  private static final RankParser RANK_PARSER = RankParser.getInstance();

  // Parser to be used
  private final EnumParser<T> parser;

  // Term ot be parsed
  private final DwcTerm term;

  /** @return a basis of record parser. */
  public static VocabularyParser<BasisOfRecord> basisOfRecordParser() {
    return new VocabularyParser<>(BOR_PARSER, DwcTerm.basisOfRecord);
  }

  /** @return a sex parser. */
  public static VocabularyParser<Sex> sexParser() {
    return new VocabularyParser<>(SEX_PARSER, DwcTerm.sex);
  }

  /** @return a life stage parser. */
  public static VocabularyParser<LifeStage> lifeStageParser() {
    return new VocabularyParser<>(LST_PARSER, DwcTerm.lifeStage);
  }

  /** @return a establishmentMeans parser. */
  public static VocabularyParser<EstablishmentMeans> establishmentMeansParser() {
    return new VocabularyParser<>(EST_PARSER, DwcTerm.establishmentMeans);
  }

  /** @return a type status parser. */
  public static VocabularyParser<TypeStatus> typeStatusParser() {
    return new VocabularyParser<>(TYPE_PARSER, DwcTerm.typeStatus);
  }

  /** @return a country parser. */
  public static VocabularyParser<Country> countryParser() {
    return new VocabularyParser<>(COUNTRY_PARSER, DwcTerm.country);
  }

  /** @return a country parser. */
  public static VocabularyParser<Country> countryCodeParser() {
    return new VocabularyParser<>(COUNTRY_PARSER, DwcTerm.countryCode);
  }

  /** @return a continent parser. */
  public static VocabularyParser<Continent> continentParser() {
    return new VocabularyParser<>(CONTINENT_PARSER, DwcTerm.continent);
  }

  /**
   * Runs a parsing method on a extended record.
   *
   * @param er to be used as input
   * @param onParse consumer called during parsing
   */
  public void parse(ExtendedRecord er, Consumer<ParseResult<T>> onParse) {
    Optional.ofNullable(extractValue(er, term))
        .ifPresent(value -> onParse.accept(parser.parse(value)));
  }

  /** @return a type status parser. */
  public static VocabularyParser<Rank> rankParser() {
    return new VocabularyParser<>(RANK_PARSER, DwcTerm.taxonRank);
  }

  /** @return a type status parser. */
  public static VocabularyParser<Rank> verbatimTaxonRankParser() {
    return new VocabularyParser<>(RANK_PARSER, DwcTerm.verbatimTaxonRank);
  }

  /**
   * Runs a parsing method on a extended record and applies a mapping function to the result.
   *
   * @param extendedRecord to be used as input
   * @param mapper function mapper
   */
  public <U> Optional<U> map(ExtendedRecord extendedRecord, Function<ParseResult<T>, U> mapper) {
    return map(extendedRecord.getCoreTerms(), mapper);
  }

  /**
   * Runs a parsing method on a map of terms and applies a mapping function to the result.
   *
   * @param terms to be used as input
   * @param mapper function mapper
   */
  public <U> Optional<U> map(Map<String, String> terms, Function<ParseResult<T>, U> mapper) {
    return Optional.ofNullable(terms.get(term.qualifiedName()))
        .map(value -> mapper.apply(parser.parse(value)));
  }
}
