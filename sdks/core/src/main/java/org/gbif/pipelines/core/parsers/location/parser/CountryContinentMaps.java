package org.gbif.pipelines.core.parsers.location.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;

/** Map countries to possible continents. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CountryContinentMaps {

  private static final String COUNTRY_CONTINENT_MAP_FILE = "country-continent-map.txt";

  // Countries are on one or two continents.
  private static final Map<Country, List<Continent>> COUNTRY_TO_CONTINENTS =
      new EnumMap(Country.class);

  static {
    ClassLoader classLoader = CountryContinentMaps.class.getClassLoader();
    try (InputStream in = classLoader.getResourceAsStream(COUNTRY_CONTINENT_MAP_FILE);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      reader
          .lines()
          .filter(nextLine -> !nextLine.isEmpty() && !nextLine.startsWith("#"))
          .map(nextLine -> nextLine.split(","))
          .forEach(
              countries -> {
                Country country = Country.fromIsoCode(countries[0].trim().toUpperCase());
                Continent continent = Continent.fromString(countries[1].trim().toUpperCase());
                add(country, continent);
              });
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Can't read [" + COUNTRY_CONTINENT_MAP_FILE + "] - aborting " + e.getMessage());
    }
  }

  private static void add(Country country, Continent continent) {
    COUNTRY_TO_CONTINENTS.putIfAbsent(country, new ArrayList<>());

    List<Continent> countrySet = COUNTRY_TO_CONTINENTS.get(country);
    countrySet.add(continent);
  }

  public static List<Continent> continentsForCountry(final Country country) {
    return COUNTRY_TO_CONTINENTS.get(country);
  }
}
