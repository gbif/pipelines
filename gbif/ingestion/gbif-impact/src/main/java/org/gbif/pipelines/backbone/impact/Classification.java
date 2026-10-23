package org.gbif.pipelines.backbone.impact;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Row;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * A classification container intended for use when classifications are to be compared for equality;
 * specifically to compare existing classifications seen on occurrence records to the classification
 * proposed by a lookup service.
 */
@Getter
@Setter
@EqualsAndHashCode
public class Classification {

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
  static Classification error() {
    Classification c = new Classification();
    c.scientificName = "ERROR";
    c.kingdom = "ERROR";
    c.setKingdomKey("-1");
    c.taxonKey = "-1";
    return c;
  }

  /**
   * @return A new classification representing unknown.
   */
  static Classification newIncertaeSedis() {
    Classification c = new Classification();
    c.scientificName = "incertae sedis";
    c.taxonKey = "0";
    return c;
  }

  /**
   * Builder for content represented in the source table using GBIF occurrence_hdfs naming
   * convention. All columns are expected as strings.
   */
  static Classification buildFromRow(Row source) {

    Classification c = new Classification();

    c.kingdom = source.getAs("kingdom");
    c.phylum = source.getAs("phylum");
    c.klass = source.getAs("class");
    c.order = source.getAs("order");
    c.superFamily = source.getAs("superfamily");
    c.family = source.getAs("family");
    c.subFamily = source.getAs("subfamily");
    c.tribe = source.getAs("tribe");
    c.subTribe = source.getAs("subtribe");
    c.genus = source.getAs("genus");
    c.subGenus = source.getAs("subgenus");
    c.species = source.getAs("species");
    c.scientificName = source.getAs("scientificname");
    c.acceptedScientificName = source.getAs("acceptedscientificname");
    c.kingdomKey = source.getAs("kingdomkey");
    c.phylumKey = source.getAs("phylumkey");
    c.classKey = source.getAs("classkey");
    c.orderKey = source.getAs("orderkey");
    c.familyKey = source.getAs("familykey");
    c.genusKey = source.getAs("genuskey");
    c.subGenusKey = source.getAs("subgenuskey");
    c.speciesKey = source.getAs("specieskey");
    c.taxonKey = source.getAs("taxonkey");
    c.acceptedTaxonKey = source.getAs("acceptedtaxonkey");
    return c;
  }

  /** Builder from a lookup web service response. */
  public static Classification buildFromNameUsageMatch(NameUsageMatchResponse usageMatch) {
    Classification c = new Classification();
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

  private static final String[] FIELDS = {
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

  private static final String[] KEYS = {
    "kingdomKey", "phylumKey", "classKey", "orderKey", "familyKey",
    "genusKey", "subGenusKey", "speciesKey", "taxonKey", "acceptedTaxonKey"
  };

  /**
   * Generates the classification column names.
   *
   * @param prefix Prefix to apply to each column name.
   * @param skipKeys If true, key-based columns are skipped.
   * @return Column names, in the same order as {@link #values(boolean)}.
   */
  public static String[] headers(String prefix, boolean skipKeys) {
    Stream<String> names = Arrays.stream(FIELDS);
    if (!skipKeys) {
      names = Stream.concat(names, Arrays.stream(KEYS));
    }
    return names.map(n -> prefix + n).toArray(String[]::new);
  }

  /**
   * @return classification values optionally skipping keys, in the order of {@link #headers(String,
   *     boolean)}
   */
  public List<String> values(boolean skipKeys) {
    Stream<String> values =
        Stream.of(
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
            acceptedScientificName);
    if (!skipKeys) {
      values =
          Stream.concat(
              values,
              Stream.of(
                  kingdomKey,
                  phylumKey,
                  classKey,
                  orderKey,
                  familyKey,
                  genusKey,
                  subGenusKey,
                  speciesKey,
                  taxonKey,
                  acceptedTaxonKey));
    }
    return values.collect(Collectors.toList());
  }

  /**
   * An equals implementation that uses all fields except the keys, optionally ignoring whitespace.
   */
  public boolean classificationEquals(
      Object o, boolean ignoreWhitespace, boolean ignoreAuthorshipFormatting) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Classification that = (Classification) o;

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
