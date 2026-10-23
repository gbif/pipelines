# GBIF Impact

This diagnostic Spark job is used by data managers to assess the impact
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
   FROM prod_b.occurrence
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
   FROM prod_b.occurrence
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


Execute the job with `spark-submit` (`--skipKeys=true` can be added to omit taxa keys in the result):
```
spark-submit \
  --class org.gbif.pipelines.backbone.impact.GbifImpactPipeline \
  --master yarn --executor-memory 4G --executor-cores 2 --num-executors 100 \
  gbif-impact-<version>-shaded.jar \
  --database=gbif_impact \
  --table=alltaxa \
  --metastoreUris=thrift://c4hivemetastore.gbif-uat.org:9083 \
  --targetDir=hdfs:///tmp/gbif-impact/report \
  --APIBaseURI=http://api.gbif-uat.org/v1/ \
  --checklistKey=<checklist key> \
  --scope=1 \
  --minimumOccurrenceCount=1000 \
  --numPartitions=200 \
  --skipKeys=false
```

Instead of a Hive table, the source can be read directly from Parquet files with
`--sourcePath=hdfs:///path/to/alltaxa` (`--database`, `--table` and `--metastoreUris` are then not needed).
`--numPartitions` controls the parallelism of the lookups against the API.

The job writes:

* `<targetDir>/impact/` – the changed classifications as Parquet
* `<targetDir>/report.csv` – the same content as a single tab delimited file with a header
  (the file name can be changed with `--reportFileName`)

Get the report:
```
hdfs dfs -get /tmp/gbif-impact/report/report.csv ./report-1000.tsv
```

The Docker image `docker.gbif.org/gbif-impact` contains the shaded jar at `/jobs/gbif-impact.jar`.
