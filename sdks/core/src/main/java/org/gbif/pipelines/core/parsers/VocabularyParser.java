package org.gbif.pipelines.core.parsers;

import static org.gbif.pipelines.core.utils.ModelUtils.DEFAULT_SEPARATOR;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.CountryParser;
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
   * @param value to be used as input
   * @param onParse consumer called during parsing
   */
  public void parse(String value, Consumer<ParseResult<T>> onParse) {
    Optional.ofNullable(value).ifPresent(r -> onParse.accept(parser.parse(r)));
  }

  /**
   * Runs a parsing method on a extended record.
   *
   * @param er to be used as input
   * @param onParse consumer called during parsing
   */
  public void parse(ExtendedRecord er, Consumer<ParseResult<T>> onParse) {
    parse(extractValue(er, term), onParse);
  }

  /**
   * Runs a parsing method on a extended record and applies a mapping function to the result.
   *
   * @param extendedRecord to be used as input
   * @param mapper function mapper
   */
  public <U> Optional<U> map(ExtendedRecord extendedRecord, Function<ParseResult<T>, U> mapper) {
    return extractOptValue(extendedRecord, term).map(value -> mapper.apply(parser.parse(value)));
  }

  /**
   * Runs a parsing method on a extended record and applies a list mapping function to the result.
   *
   * @param extendedRecord to be used as input
   * @param mapper function mapper
   */
  public <U> Optional<U> mapList(
      ExtendedRecord extendedRecord, Function<ParseResult<T>, U> mapper) {
    return mapList(extractValue(extendedRecord, term), mapper);
  }

  /**
   * Runs a parsing method on a map of a multivalue term and applies a mapping function to each of
   * the values.
   *
   * @param rawValue to be used as input
   * @param mapper function mapper
   */
  public <U> Optional<U> mapList(String rawValue, Function<ParseResult<T>, U> mapper) {
    if (rawValue == null || rawValue.isEmpty()) {
      return Optional.empty();
    }

    Set<String> values =
        Stream.of(rawValue.split(DEFAULT_SEPARATOR))
            .map(String::trim)
            .filter(v -> !v.isEmpty())
            .collect(Collectors.toSet());

    U result = null;
    for (String v : values) {
      result = mapper.apply(parser.parse(v));
    }

    return Optional.ofNullable(result);
  }
}
