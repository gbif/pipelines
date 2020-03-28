# Ingest-fragments uploader

[Fragments uploader](src/main/java/org/gbif/pipelines/fragmenter/FragmentsUploader.java) - reads dwca/xml based archive and uploads raw json/xml records into HBase table.

HBase table has following structure:

- Table (Table)
    - **fragment** (Column family)
        - **datasetKey** (Qualifier)
        - **attempt** (Qualifier)
        - **protocol** (Qualifier)
        - **record** (Qualifier)
        - **dateCreated** (Qualifier)
        - **dateUpdated** (Qualifier)

Processing workflow:
1. Read a dwca/xml archive
2. Collect raw records into small batches (batch size is configurable)
3. Get or create GBIF id for each element of the batch and create keys (salt + ":" + GBIF id)
4. Get **dateCreated** from the table using GBIF id, if a record already exists
5. Create HBase put(create new or update existing) records and upload them into HBase

How to use:
```java
    long recordsProcessed = FragmentsUploader.dwcaBuilder()
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
         .upload();
 ```
