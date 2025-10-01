package org.gbif.pipelines.backbone.impact;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * A classification container intended for use when classifications are to be compared for equality;
 * specifically to compare existing classifications seen on occurrence records to the classification
 * proposed by a lookup service.
 */
@Getter
@Setter
@EqualsAndHashCode
public class GBIFClassification {

  private String kingdom;
  private String phylum;
  private String klass;
  private String order;
  private String superFamily;
  private String family;
  private String subFamily;
  private String tribe;
  private String subTribe;
  private String genus;
  private String subGenus;
  private String species;
  private String scientificName;
  private String acceptedScientificName;
  private String kingdomKey;
  private String phylumKey;
  private String classKey;
  private String orderKey;
  private String familyKey;
  private String genusKey;
  private String subGenusKey;
  private String speciesKey;
  private String taxonKey;
  private String acceptedTaxonKey;

  /**
   * @return A new classification representing unknown.
   */
  static GBIFClassification error() {
    GBIFClassification c = new GBIFClassification();
    c.scientificName = "ERROR";
    c.kingdom = "ERROR";
    c.setKingdomKey("-1");
    c.taxonKey = "-1";
    return c;
  }

  /**
   * @return A new classification representing unknown.
   */
  static GBIFClassification newIncertaeSedis() {
    GBIFClassification c = new GBIFClassification();
    c.scientificName = "incertae sedis";
    c.taxonKey = "0";
    return c;
  }

  /**
   * Builder for content represented in Hive sourced data using GBIF occurrence_hdfs naming
   * convention.
   */
  static GBIFClassification buildFromHiveSource(HCatRecord source, HCatSchema schema)
      throws HCatException {

    GBIFClassification c = new GBIFClassification();

    c.kingdom = source.getString("kingdom", schema);
    c.phylum = source.getString("phylum", schema);
    c.klass = source.getString("class", schema);
    c.order = source.getString("order", schema);
    c.superFamily = source.getString("superfamily", schema);
    c.family = source.getString("family", schema);
    c.subFamily = source.getString("subfamily", schema);
    c.tribe = source.getString("tribe", schema);
    c.subTribe = source.getString("subtribe", schema);
    c.genus = source.getString("genus", schema);
    c.subGenus = source.getString("subGenus", schema);
    c.species = source.getString("species", schema);
    c.scientificName = source.getString("scientificname", schema);
    c.acceptedScientificName = source.getString("acceptedscientificname", schema);
    c.kingdomKey = source.getString("kingdomkey", schema) + "";
    c.phylumKey = source.getString("phylumkey", schema) + "";
    c.classKey = source.getString("classkey", schema) + "";
    c.orderKey = source.getString("orderkey", schema) + "";
    c.familyKey = source.getString("familykey", schema) + "";
    c.genusKey = source.getString("genuskey", schema) + "";
    c.subGenusKey = source.getString("subgenuskey", schema) + "";
    c.speciesKey = source.getString("specieskey", schema) + "";
    c.taxonKey = source.getString("taxonkey", schema) + "";
    c.acceptedTaxonKey = source.getString("acceptedtaxonkey", schema) + "";
    return c;
  }

  /** Builder from a lookup web service response. */
  public static GBIFClassification buildFromNameUsageMatch(NameUsageMatchResponse usageMatch) {
    GBIFClassification c = new GBIFClassification();
    if (Objects.nonNull(usageMatch.getClassification())) {
      usageMatch
          .getClassification()
          .forEach(
              rankedName -> {
                switch (rankedName.getRank()) {
                  case "KINGDOM":
                    c.kingdom = rankedName.getName();
                    c.kingdomKey = rankedName.getKey() + "";
                    break;
                  case "PHYLUM":
                    c.phylum = rankedName.getName();
                    c.phylumKey = rankedName.getKey() + "";
                    break;
                  case "CLASS":
                    c.klass = rankedName.getName();
                    c.classKey = rankedName.getKey() + "";
                    break;
                  case "ORDER":
                    c.order = rankedName.getName();
                    c.orderKey = rankedName.getKey() + "";
                    break;
                  case "SUPERFAMILY":
                    c.superFamily = rankedName.getName();
                    // superFamilyKey is not used in GBIFClassification
                    break;
                  case "FAMILY":
                    c.family = rankedName.getName();
                    c.familyKey = rankedName.getKey() + "";
                    break;
                  case "SUBFAMILY":
                    c.subFamily = rankedName.getName();
                    // subFamilyKey is not used in GBIFClassification
                    break;
                  case "TRIBE":
                    c.tribe = rankedName.getName();
                    // tribeKey is not used in GBIFClassification
                    break;
                  case "SUBTRIBE":
                    c.subTribe = rankedName.getName();
                    // subTribeKey is not used in GBIFClassification
                    break;
                  case "GENUS":
                    c.genus = rankedName.getName();
                    c.genusKey = rankedName.getKey() + "";
                    break;
                  case "SUBGENUS":
                    c.subGenus = rankedName.getName();
                    c.subGenusKey = rankedName.getKey() + "";
                    break;
                  case "SPECIES":
                    c.species = rankedName.getName();
                    c.speciesKey = rankedName.getKey() + "";
                    break;
                  default:
                    break;
                }
              });

      if (usageMatch.getUsage() != null) {
        c.scientificName = usageMatch.getUsage().getName();
        c.taxonKey = usageMatch.getUsage().getKey() + "";
      }

      if (usageMatch.getAcceptedUsage() != null) {
        c.acceptedScientificName = usageMatch.getAcceptedUsage().getName();
        c.acceptedTaxonKey = usageMatch.getAcceptedUsage().getKey() + "";
      } else if (usageMatch.getUsage() != null) {
        c.acceptedScientificName = usageMatch.getUsage().getName();
        c.acceptedTaxonKey = usageMatch.getUsage().getKey() + "";
      }
    }

    return c;
  }

  /**
   * @return classification in tab delimited format
   */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Generates a tab-delimited list of classification headers.
   *
   * @param prefix Prefix to apply to each header field.
   * @param skipKeys If true, key-based headers are skipped.
   * @return Tab-delimited string of classification headers.
   */
  public static String toHeader(String prefix, boolean skipKeys) {
    String[] fields = {
      "kingdom",
      "phylum",
      "klass",
      "order",
      "superFamily",
      "family",
      "subFamily",
      "tribe",
      "subTribe",
      "genus",
      "subGenus",
      "species",
      "scientificName",
      "acceptedScientificName"
    };

    String[] keys = {
      "kingdomKey", "phylumKey", "classKey", "orderKey", "familyKey",
      "genusKey", "subGenusKey", "speciesKey", "taxonKey", "acceptedTaxonKey"
    };

    Stream<String> headerStream = Arrays.stream(fields).map(f -> prefix + f);

    if (!skipKeys) {
      headerStream = Stream.concat(headerStream, Arrays.stream(keys).map(k -> prefix + k));
    }

    return headerStream.collect(Collectors.joining("\t"));
  }

  /**
   * @return classification in tab delimited format optionally skipping keys
   */
  public String toString(boolean skipKeys) {
    CharSequence[] defaultValues = {
      kingdom,
      phylum,
      klass,
      order,
      superFamily,
      family,
      subFamily,
      tribe,
      subTribe,
      genus,
      subGenus,
      species,
      scientificName,
      acceptedScientificName
    };

    if (skipKeys) return String.join("\t", defaultValues);
    else {
      CharSequence[] keyVals = {
        safe(kingdomKey),
        safe(phylumKey),
        safe(classKey),
        safe(orderKey),
        safe(familyKey),
        safe(genusKey),
        safe(subGenusKey),
        safe(speciesKey),
        safe(taxonKey),
        safe(acceptedTaxonKey)
      };
      CharSequence[] allVals =
          Stream.concat(Arrays.stream(defaultValues), Arrays.stream(keyVals))
              .toArray(CharSequence[]::new);

      return String.join("\t", allVals);
    }
  }

  /** Returns the string or an empty string if null. */
  private static String safe(String value) {
    //    return value != null && !value.equalsIgnoreCase("null") ? value : "";
    return value;
  }

  /**
   * An equals implementation that uses all fields except the keys, optionally ignoring whitespace.
   */
  public boolean classificationEquals(
      Object o, boolean ignoreWhitespace, boolean ignoreAuthorshipFormatting) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GBIFClassification that = (GBIFClassification) o;

    if (!ignoreWhitespace) {
      return Objects.equals(kingdom, that.kingdom)
          && Objects.equals(phylum, that.phylum)
          && Objects.equals(klass, that.klass)
          && Objects.equals(order, that.order)
          //            && Objects.equals(superFamily, that.superFamily)
          && Objects.equals(family, that.family)
          //            && Objects.equals(subFamily, that.subFamily)
          //            && Objects.equals(tribe, that.tribe)
          //            && Objects.equals(subTribe, that.subTribe)
          && Objects.equals(genus, that.genus)
          && Objects.equals(subGenus, that.subGenus)
          && Objects.equals(species, that.species)
          && Objects.equals(scientificName, that.scientificName)
          && Objects.equals(acceptedScientificName, that.acceptedScientificName);
    } else {
      return lenientEquals(ignoreAuthorshipFormatting, kingdom, that.kingdom)
          && lenientEquals(ignoreAuthorshipFormatting, phylum, that.phylum)
          && lenientEquals(ignoreAuthorshipFormatting, klass, that.klass)
          && lenientEquals(ignoreAuthorshipFormatting, order, that.order)
          && lenientEquals(ignoreAuthorshipFormatting, family, that.family)
          && lenientEquals(ignoreAuthorshipFormatting, genus, that.genus)
          && lenientEquals(ignoreAuthorshipFormatting, subGenus, that.subGenus)
          && lenientEquals(ignoreAuthorshipFormatting, species, that.species)
          && lenientEquals(ignoreAuthorshipFormatting, scientificName, that.scientificName)
          && lenientEquals(
              ignoreAuthorshipFormatting, acceptedScientificName, that.acceptedScientificName);
    }
  }

  /** returns true if both are null or they are the same without whitespace, ignoring case. */
  public static boolean lenientEquals(boolean ignoreAuthorshipFormatting, String s1, String s2) {
    if (s1 == null || s2 == null) {
      return s1 == null && s2 == null;
    } else {
      String s1c = s1.replaceAll(" ", "");
      String s2c = s2.replaceAll(" ", "");
      if (ignoreAuthorshipFormatting) {
        s1c = s1c.replaceAll("[\\s(),\"']", "");
        s2c = s2c.replaceAll("[\\s(),\"']", "");
      }
      return s1c.equalsIgnoreCase(s2c);
    }
  }
}
