package org.gbif.pipelines.core.parsers.legacy;

import org.gbif.api.vocabulary.Country;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps of countries that are commonly confused, or are considered equivalent.
 */
public class CountryMaps {

  private static final Logger LOG = LoggerFactory.getLogger(CountryMaps.class);

  private static final String CONFUSED_COUNTRY_FILE = "confused-country-pairs.txt";

  private CountryMaps() {}

  /*
   Some countries are commonly mislabeled and are close to correct, so we want to accommodate them, e.g. Northern
   Ireland mislabeled as Ireland (should be GB). Add comma separated pairs of acceptable country swaps in the
   confused_country_pairs.txt file. Entries will be made in both directions (e.g. IE->GB, GB->IE). Multiple entries per
   country are allowed (e.g. AB,CD and AB,EF).
   This *overrides* the provided country, and includes an issue.
  */
  private static final Map<Country, Set<Country>> CONFUSED_COUNTRIES = Maps.newHashMap();
  // And this is the same, but without the issue — we aren't exactly following ISO, but we accept it.
  private static final Map<Country, Set<Country>> EQUIVALENT_COUNTRIES = Maps.newHashMap();

  static {
    try (InputStream in = CountryMaps.class.getClassLoader().getResourceAsStream(CONFUSED_COUNTRY_FILE);
         BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
      String nextLine;
      while ((nextLine = reader.readLine()) != null) {
        if (!nextLine.isEmpty() && !nextLine.startsWith("#")) {
          String[] countries = nextLine.split(",");
          Country countryA = Country.fromIsoCode(countries[0].trim().toUpperCase());
          Country countryB = Country.fromIsoCode(countries[1].trim().toUpperCase());
          boolean addIssue = Boolean.parseBoolean(countries[2].trim());
          LOG.info("Adding [{}][{}] ({}) pair to confused country matches.", countryA, countryB, addIssue ? "with issue" : "without issue");
          addConfusedCountry(countryA, countryB, addIssue);
          addConfusedCountry(countryB, countryA, addIssue);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't read [" + CONFUSED_COUNTRY_FILE + "] - aborting", e);
    } finally {
      try {
        reader.close();
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Couldn't close [{}] - continuing anyway", CONFUSED_COUNTRY_FILE, e);
      }
    }
  }

  private static void addConfusedCountry(Country countryA, Country countryB, boolean withIssue) {
    Map<Country, Set<Country>> map = withIssue ? CONFUSED_COUNTRIES : EQUIVALENT_COUNTRIES;

    if (!map.containsKey(countryA)) {
      map.put(countryA, Sets.<Country>newHashSet());
    }

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
