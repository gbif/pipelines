package org.gbif.pipelines.backbone.impact;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.gbif.api.service.checklistbank.NameParser;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.rest.client.species.NameUsageMatch;

import java.util.Objects;

/**
 * A classification container intended for use when classifications are to be compared for equality;
 * specifically to compare existing classifications seen on occurrence records to the classification
 * proposed by a lookup service.
 */
@Getter
@Setter
@EqualsAndHashCode
class GBIFClassification {
  private String kingdom;
  private String phylum;
  private String klass;
  private String order;
  private String family;
  private String genus;
  private String subGenus;
  private String species;
  private String scientificName;
  private String acceptedScientificName;
  private Integer kingdomKey;
  private Integer phylumKey;
  private Integer classKey;
  private Integer orderKey;
  private Integer familyKey;
  private Integer genusKey;
  private Integer subGenusKey;
  private Integer speciesKey;
  private Integer taxonKey;
  private Integer acceptedTaxonKey;

  /** @return A new classification representing unknown. */
  static GBIFClassification newIncertaeSedis() {
    GBIFClassification c = new GBIFClassification();
    c.scientificName = "incertae sedis";
    c.taxonKey = 0;
    return c;
  }

  /**
   * Builder for content represented in Hive sourced data using GBIF occurrence_hdfs naming
   * convention.
   */
  static GBIFClassification buildFromHive(HCatRecord source, HCatSchema schema)
      throws HCatException {
    GBIFClassification c = new GBIFClassification();
    c.kingdom = source.getString("kingdom", schema);
    c.phylum = source.getString("phylum", schema);
    c.klass = source.getString("class", schema);
    c.order = source.getString("order_", schema);
    c.family = source.getString("family", schema);
    c.genus = source.getString("genus", schema);
    c.subGenus = source.getString("subGenus", schema);
    c.species = source.getString("species", schema);
    c.scientificName = source.getString("scientificName", schema);
    c.acceptedScientificName = source.getString("acceptedScientificName", schema);
    c.kingdomKey = source.getInteger("kingdomKey", schema);
    c.phylumKey = source.getInteger("phylumKey", schema);
    c.classKey = source.getInteger("classKey", schema);
    c.orderKey = source.getInteger("orderKey", schema);
    c.familyKey = source.getInteger("familyKey", schema);
    c.genusKey = source.getInteger("genusKey", schema);
    c.subGenusKey = source.getInteger("subGenusKey", schema);
    c.speciesKey = source.getInteger("speciesKey", schema);
    c.taxonKey = source.getInteger("taxonKey", schema);
    c.acceptedTaxonKey = source.getInteger("acceptedTaxonKey", schema);
    return c;
  }

  /** Builder from a lookup web service response. */
  static GBIFClassification buildFromNameUsageMatch(NameUsageMatch usageMatch) {
    GBIFClassification c = new GBIFClassification();
    if (Objects.nonNull(usageMatch.getClassification())) {
      usageMatch
          .getClassification()
          .forEach(
              rankedName -> {
                switch (rankedName.getRank()) {
                  case KINGDOM:
                    c.kingdom = rankedName.getName();
                    c.kingdomKey = rankedName.getKey();
                    break;
                  case PHYLUM:
                    c.phylum = rankedName.getName();
                    c.phylumKey = rankedName.getKey();
                    break;
                  case CLASS:
                    c.klass = rankedName.getName();
                    c.classKey = rankedName.getKey();
                    break;
                  case ORDER:
                    c.order = rankedName.getName();
                    c.orderKey = rankedName.getKey();
                    break;
                  case FAMILY:
                    c.family = rankedName.getName();
                    c.familyKey = rankedName.getKey();
                    break;
                  case GENUS:
                    c.genus = rankedName.getName();
                    c.genusKey = rankedName.getKey();
                    break;
                  case SUBGENUS:
                    c.subGenus = rankedName.getName();
                    c.subGenusKey = rankedName.getKey();
                    break;
                  case SPECIES:
                    c.species = rankedName.getName();
                    c.speciesKey = rankedName.getKey();
                    break;
                  default:
                    break;
                }
              });

      if (usageMatch.getUsage() != null) {
        c.scientificName = usageMatch.getUsage().getName();
        c.taxonKey = usageMatch.getUsage().getKey();
      }

      if (usageMatch.getAcceptedUsage() != null) {
        c.acceptedScientificName = usageMatch.getAcceptedUsage().getName();
        c.acceptedTaxonKey = usageMatch.getAcceptedUsage().getKey();

      } else if (usageMatch.getUsage() != null) {
        c.acceptedScientificName = usageMatch.getUsage().getName();
        c.acceptedTaxonKey = usageMatch.getUsage().getKey();
      }
    }

    return c;
  }

  /** @return order in tab delimited format */
  @Override
  public String toString() {
    return String.join(
        "\t",
        kingdom,
        phylum,
        klass,
        order,
        family,
        genus,
        subGenus,
        species,
        scientificName,
        acceptedScientificName,
        String.valueOf(kingdomKey),
        String.valueOf(phylumKey),
        String.valueOf(classKey),
        String.valueOf(orderKey),
        String.valueOf(familyKey),
        String.valueOf(genusKey),
        String.valueOf(subGenusKey),
        String.valueOf(speciesKey),
        String.valueOf(taxonKey),
        String.valueOf(acceptedTaxonKey));
  }

  /** Utility container for capturing the ranks at which difference occur in two classifications. */
  @Getter
  @Setter
  @Deprecated // Currently moved into the visualisation layer, and could be removed
  static class RankOfDiff {
    private static final NameParser PARSER = new NameParserGbifV1();

    private String scientificName;
    private String canonicalName;
    private String identifier;
    private boolean acceptedScientificNameChanged;
    private boolean acceptedCanonicalNameChanged;
    private boolean acceptedIdentifierChanged;

    // Not pretty but written to be simple to follow
    static RankOfDiff difference(GBIFClassification c1, GBIFClassification c2) {
      RankOfDiff d = new RankOfDiff();

      // has the accepted name changed at all
      d.acceptedScientificNameChanged =
          !Objects.equals(c1.acceptedScientificName, c2.acceptedScientificName);
      d.acceptedCanonicalNameChanged =
          !Objects.equals(
              PARSER.parseToCanonical(c1.acceptedScientificName),
              PARSER.parseToCanonical(c2.acceptedScientificName));
      d.acceptedIdentifierChanged = !Objects.equals(c1.acceptedTaxonKey, c2.acceptedTaxonKey);

      // the highest rank we see changes in the scientific name
      d.scientificName =
          Objects.equals(c1.scientificName, c2.scientificName)
              ? d.scientificName
              : "SCIENTIFIC_NAME";
      d.scientificName = Objects.equals(c1.species, c2.species) ? d.scientificName : "SPECIES";
      d.scientificName = Objects.equals(c1.subGenus, c2.subGenus) ? d.scientificName : "SUB_GENUS";
      d.scientificName = Objects.equals(c1.genus, c2.genus) ? d.scientificName : "GENUS";
      d.scientificName = Objects.equals(c1.family, c2.family) ? d.scientificName : "FAMILY";
      d.scientificName = Objects.equals(c1.order, c2.order) ? d.scientificName : "ORDER";
      d.scientificName = Objects.equals(c1.klass, c2.klass) ? d.scientificName : "CLASS";
      d.scientificName = Objects.equals(c1.phylum, c2.phylum) ? d.scientificName : "PHYLUM";
      d.scientificName = Objects.equals(c1.kingdom, c2.kingdom) ? d.scientificName : "KINGDOM";

      // the highest rank we see changes in the canonical name
      d.canonicalName =
          Objects.equals(
                  PARSER.parseToCanonical(c1.scientificName),
                  PARSER.parseToCanonical(c2.scientificName))
              ? d.canonicalName
              : "SCIENTIFIC_NAME";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.species), PARSER.parseToCanonical(c2.species))
              ? d.canonicalName
              : "SPECIES";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.subGenus), PARSER.parseToCanonical(c2.subGenus))
              ? d.canonicalName
              : "SUB_GENUS";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.genus), PARSER.parseToCanonical(c2.genus))
              ? d.canonicalName
              : "GENUS";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.family), PARSER.parseToCanonical(c2.family))
              ? d.canonicalName
              : "FAMILY";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.order), PARSER.parseToCanonical(c2.order))
              ? d.canonicalName
              : "ORDER";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.klass), PARSER.parseToCanonical(c2.klass))
              ? d.canonicalName
              : "CLASS";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.phylum), PARSER.parseToCanonical(c2.phylum))
              ? d.canonicalName
              : "PHYLUM";
      d.canonicalName =
          Objects.equals(PARSER.parseToCanonical(c1.kingdom), PARSER.parseToCanonical(c2.kingdom))
              ? d.canonicalName
              : "KINGDOM";

      // the highest rank we see changes in the identifiers
      d.identifier = Objects.equals(c1.taxonKey, c2.taxonKey) ? d.identifier : "SCIENTIFIC_NAME";
      d.identifier = Objects.equals(c1.speciesKey, c2.speciesKey) ? d.identifier : "SPECIES";
      d.identifier = Objects.equals(c1.subGenusKey, c2.subGenusKey) ? d.identifier : "SUB_GENUS";
      d.identifier = Objects.equals(c1.genusKey, c2.genusKey) ? d.identifier : "GENUS";
      d.identifier = Objects.equals(c1.familyKey, c2.familyKey) ? d.identifier : "FAMILY";
      d.identifier = Objects.equals(c1.orderKey, c2.orderKey) ? d.identifier : "ORDER";
      d.identifier = Objects.equals(c1.classKey, c2.classKey) ? d.identifier : "CLASS";
      d.identifier = Objects.equals(c1.phylumKey, c2.phylumKey) ? d.identifier : "PHYLUM";
      d.identifier = Objects.equals(c1.kingdomKey, c2.kingdomKey) ? d.identifier : "KINGDOM";

      return d;
    }
  }
}
