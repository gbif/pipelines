package org.gbif.pipelines.core.parsers.location.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Country;

/** Maps of countries that are commonly confused, or are considered equivalent. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CountryMaps {

  private static final String CONFUSED_COUNTRY_FILE = "confused-country-pairs.txt";

  /*
   Some countries are commonly mislabeled and are close to correct, so we want to accommodate them, e.g. Northern
   Ireland mislabeled as Ireland (should be GB). Add comma separated pairs of acceptable country swaps in the
   confused_country_pairs.txt file. Entries will be made in both directions (e.g. IE->GB, GB->IE). Multiple entries per
   country are allowed (e.g. AB,CD and AB,EF).
   This *overrides* the provided country, and includes an issue.
  */
  private static final Map<Country, Set<Country>> CONFUSED_COUNTRIES = new EnumMap<>(Country.class);
  // And this is the same, but without the issue — we aren't exactly following ISO, but we accept
  // it.
  private static final Map<Country, Set<Country>> EQUIVALENT_COUNTRIES =
      new EnumMap<>(Country.class);

  static {
    ClassLoader classLoader = CountryMaps.class.getClassLoader();
    try (InputStream in = classLoader.getResourceAsStream(CONFUSED_COUNTRY_FILE);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      reader
          .lines()
          .filter(nextLine -> !nextLine.isEmpty() && !nextLine.startsWith("#"))
          .map(nextLine -> nextLine.split(","))
          .forEach(
              countries -> {
                Country countryA = Country.fromIsoCode(countries[0].trim().toUpperCase());
                Country countryB = Country.fromIsoCode(countries[1].trim().toUpperCase());
                boolean addIssue = Boolean.parseBoolean(countries[2].trim());
                addConfusedCountry(countryA, countryB, addIssue);
                addConfusedCountry(countryB, countryA, addIssue);
              });
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Can't read [" + CONFUSED_COUNTRY_FILE + "] - aborting " + e.getMessage());
    }
  }

  private static void addConfusedCountry(Country countryA, Country countryB, boolean withIssue) {
    Map<Country, Set<Country>> map = withIssue ? CONFUSED_COUNTRIES : EQUIVALENT_COUNTRIES;

    map.putIfAbsent(countryA, new HashSet<>());

    Set<Country> confused = map.get(countryA);
    confused.add(countryA);
    confused.add(countryB);
  }

  public static Set<Country> equivalent(final Country country) {
    return EQUIVALENT_COUNTRIES.get(country);
  }

  public static Set<Country> confused(final Country country) {
    return CONFUSED_COUNTRIES.get(country);
  }
}
