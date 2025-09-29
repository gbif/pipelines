# Pre-backbone Release

This diagnostic pipeline is used by data managers to assess the impact
of a proposed taxonomy release. The verbatim occurrence
classifications are looked up against the proposed new
backbone. All records that change from their current organisation
are logged so they can be reviewed before the taxonomy is deployed.

To set up, prepare a table in Hive:

```sql
CREATE DATABASE IF NOT EXISTS gbif_impact;
DROP TABLE gbif_impact.alltaxa;
CREATE TABLE gbif_impact.alltaxa STORED AS PARQUET 
   AS SELECT
   v_kingdom,
   v_phylum,
   v_class,
   v_order,
   v_superfamily,
   v_family,
   v_subfamily,
   v_tribe,
   v_subtribe,
   v_genus,
   v_subGenus,
   v_scientificName,
   v_scientificNameAuthorship,
   v_taxonRank,
   v_verbatimTaxonRank,
   v_genericName,
   v_specificEpithet,
   v_infraSpecificEpithet,
   v_cultivarEpithet,
   v_scientificNameID,
   v_taxonID,
   v_taxonConceptID,
   kingdom,
   phylum,
   class,
   `order`,
   superfamily,
   family,
   subfamily,
   tribe,
   subtribe,
   genus,
   subGenus,
   species,
   scientificName,
   acceptedScientificName,
   CAST(kingdomKey AS STRING) AS kingdomKey,
   CAST(phylumKey AS STRING) AS phylumKey,
   CAST(classKey AS STRING) AS classKey,
   CAST(orderKey AS STRING) AS orderKey,
   CAST(familyKey AS STRING) AS familyKey,
   CAST(genusKey AS STRING) AS genusKey,
   CAST(subGenusKey AS STRING) AS subGenusKey,
   CAST(speciesKey AS STRING) AS speciesKey,
   CAST(taxonKey AS STRING) AS taxonKey,
   CAST(acceptedTaxonKey AS STRING) AS acceptedTaxonKey,
   count(*) as occurrenceCount
   FROM uat2.occurrence
   GROUP BY
   v_kingdom,
   v_phylum,
   v_class,
   v_order,
   v_superfamily,
   v_family,
   v_subfamily,
   v_tribe,
   v_subtribe,
   v_genus,
   v_subGenus,
   v_scientificName,
   v_scientificNameAuthorship,
   v_taxonRank,
   v_verbatimTaxonRank,
   v_genericName,
   v_specificEpithet,
   v_infraSpecificEpithet,
   v_cultivarEpithet,
   v_scientificNameID,
   v_taxonID,
   v_taxonConceptID,  
   kingdom,
   phylum,
   class,
   `order`,
   superfamily,
   family,
   subfamily,
   tribe,
   subtribe,
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

Additional report tables:

```sql
create table gbif_impact.coleoptera as select * from gbif_impact.alltaxa where `order` = 'Coleoptera';
create table gbif_impact.fabaceae as select * from gbif_impact.alltaxa where family = 'Fabaceae';
create table gbif_impact.fungi as select * from gbif_impact.alltaxa where kingdom = 'Fungi';
create table gbif_impact.aves as select * from gbif_impact.alltaxa where class = 'Aves';
create table gbif_impact.plantae as select * from gbif_impact.alltaxa where kingdom = 'Plantae';
```

For fossils:
```sql
DROP TABLE gbif_impact.fossils;
CREATE TABLE gbif_impact.fossils STORED AS PARQUET
   AS SELECT
   v_kingdom,
   v_phylum,
   v_class,
   v_order,
   v_superfamily,
   v_family,
   v_subfamily,
   v_tribe,
   v_subtribe,
   v_genus,
   v_subGenus,
   v_scientificName,
   v_scientificNameAuthorship,
   v_taxonRank,
   v_verbatimTaxonRank,
   v_genericName,
   v_specificEpithet,
   v_infraSpecificEpithet,
   v_cultivarEpithet,
   v_scientificNameID,
   v_taxonID,
   v_taxonConceptID,
   kingdom,
   phylum,
   class,
   `order`,
   superfamily,
   family,
   subfamily,
   tribe,
   subtribe,
   genus,
   subGenus,
   species,
   scientificName,
   acceptedScientificName,
   CAST(kingdomKey AS STRING) AS kingdomKey,
   CAST(phylumKey AS STRING) AS phylumKey,
   CAST(classKey AS STRING) AS classKey,
   CAST(orderKey AS STRING) AS orderKey,
   CAST(familyKey AS STRING) AS familyKey,
   CAST(genusKey AS STRING) AS genusKey,
   CAST(subGenusKey AS STRING) AS subGenusKey,
   CAST(speciesKey AS STRING) AS speciesKey,
   CAST(taxonKey AS STRING) AS taxonKey,
   CAST(acceptedTaxonKey AS STRING) AS acceptedTaxonKey,
   count(*) as occurrenceCount
   FROM uat2.occurrence
   WHERE basisOfRecord = 'FOSSIL_SPECIMEN'
   GROUP BY
   v_kingdom,
   v_phylum,
   v_class,
   v_order,
   v_superfamily,
   v_family,
   v_subfamily,
   v_tribe,
   v_subtribe,
   v_genus,
   v_subGenus,
   v_scientificName,
   v_scientificNameAuthorship,
   v_taxonRank,
   v_verbatimTaxonRank,
   v_genericName,
   v_specificEpithet,
   v_infraSpecificEpithet,
   v_cultivarEpithet,
   v_scientificNameID,
   v_taxonID,
   v_taxonConceptID,  
   kingdom,
   phylum,
   class,
   `order`,
   superfamily,
   family,
   subfamily,
   tribe,
   subtribe,
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


Execute the pipeline using e.g. (not --skipKeys=false can be added to omit taxa keys in the result):
```
spark2-submit \
  --class org.gbif.pipelines.backbone.impact.BackbonePreRelease \
  --master yarn --executor-memory 4G --executor-cores 2 --num-executors 100 \
  pre-backbone-release-2.14.0-SNAPSHOT-shaded.jar \
  --datebase=tim \
  --table=classifications \
  --targetDir=hdfs:///tmp/backbone-pre-release-impact/report \
  --metastoreUris=thrift://c4hivemetastore.gbif-uat.org:9083
  --APIBaseURI=http://api.gbif-uat.org/v1/
  --scope=1
  --minimumOccurrenceCount=1000
  --skipKeys=false
```

Get the result:
```
hdfs dfs -getmerge /tmp/backbone-pre-release-impact /tmp/report-1000.txt
```

Prepend a header (optionally use the header-no-keys.tsv)
```
cat header.tsv /tmp/report-1000.txt > ./report-1000.tsv
```
