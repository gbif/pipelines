# Ingest-fragmenter

[Fragments persister](src/main/java/org/gbif/pipelines/fragmenter/FragmentPersister.java) - reads dwca/xml based archive and persists raw json/xml records into HBase table.

HBase table key is - **salt:gbifId** and value has following structure:

|   |   |   |fragment(CF)|   |   |
|---|---|---|------------|---|---|
|datasetKey(Q)|attempt(Q)|protocol(Q)|record(Q)|dateCreated(Q)|dateUpdated(Q)  |
|5e4b68f2-23df-4382-8f79-ba449812f1f9|1|DWCA_ARCHIVE|CLOB|1587372235|1587372235|

Example for dev:
```
create 'dev_fragment', {NAME => 'fragment', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW'},
  {SPLITS => [
    '01','02','03','04','05','06','07','08','09','10',
    '11','12','13','14','15','16','17','18','19','20',
    '21','22','23','24','25','26','27','28','29','30',
    '31','32','33','34','35','36','37','38','39','40',
    '41','42','43','44','45','46','47','48','49','50',
    '51','52','53','54','55','56','57','58','59','60',
    '61','62','63','64','65','66','67','68','69','70',
    '71','72','73','74','75','76','77','78','79','80',
    '81','82','83','84','85','86','87','88','89','90',
    '91','92','93','94','95','96','97','98','99'
  ]}
```

Processing workflow:
1. Read a dwca/xml archive
2. Collect raw records into small batches (batch size is configurable)
3. Get or create GBIF id for each element of the batch and create keys (salt + ":" + GBIF id)
4. Get **dateCreated** from the table using key salt:gbifId, if record already exists
5. Create HBase put(create new or update existing) records and persist them into HBase

How to use:
```java
    long recordsProcessed = FragmentPersister.dwcaBuilder()
         .tableName("Tabe name")
         .keygenConfig(config)
         .pathToArchive(path)
         .useTriplet(true)
         .useOccurrenceId(true)
         .datasetKey(datasetKey)
         .attempt(attempt)
         .endpointType(EndpointType.DWC_ARCHIVE)
         .hbaseConnection(connection)
         .build()
         .persist();
```
