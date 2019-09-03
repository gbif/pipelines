# Pre-backbone Release

This diagnostic pipeline is used by data managers to assess the impact
of a proposed backbone taxonomy release. The verbatim occurrence
classifications are looked up against the proposed new
backbone. All records that change from their current organisation
are logged so they can be reviewed before the backbone is deployed.

To set up, prepare a table in Hive:
```
DROP TABLE tim.classifications;
CREATE TABLE tim.classifications
STORED AS ORC
AS SELECT
  v_kingdom,
  v_phylum,
  v_class,
  v_order,
  v_family,
  v_genus,
  v_subGenus,
  v_scientificName,
  v_scientificNameAuthorship,
  v_taxonRank,
  v_verbatimTaxonRank,
  v_specificEpithet,
  v_infraSpecificEpithet,
  kingdom,
  phylum,
  class,
  order_,
  family,
  genus,
  subGenus,
  species,
  scientificName,
  acceptedScientificName,
  kingdomKey,
  phylumKey,
  classKey,
  orderKey,
  familyKey,
  genusKey,
  subGenusKey,
  speciesKey,
  taxonKey,
  acceptedTaxonKey,
  count(*) as occurrenceCount
FROM uat.occurrence_hdfs
GROUP BY
  v_kingdom,
  v_phylum,
  v_class,
  v_order,
  v_family,
  v_genus,
  v_subGenus,
  v_scientificName,
  v_scientificNameAuthorship,
  v_taxonRank,
  v_verbatimTaxonRank,
  v_specificEpithet,
  v_infraSpecificEpithet,
  kingdom,
  phylum,
  class,
  order_,
  family,
  genus,
  subGenus,
  species,
  scientificName,
  acceptedScientificName,
  kingdomKey,
  phylumKey,
  classKey,
  orderKey,
  familyKey,
  genusKey,
  subGenusKey,
  speciesKey,
  taxonKey,
  acceptedTaxonKey;
```