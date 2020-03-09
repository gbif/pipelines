# Ingest-fragments uploader

[Fragments uploader](.src/main/java/org/gbif/pipelines/fragmenter/FragmentsUploader.java) - reads dwca/xml based archive and uploads raw json/xml records into HBase table.

HBase table has following structure:

- Table (Table)
    - **fragment** (Column family)
        - **datasetId** (Qualifier)
        - **attempt** (Qualifier)
        - **protocol** (Qualifier)
        - **record** (Qualifier)
        - **dateCreated** (Qualifier)
        - **dateUpdated** (Qualifier)

Processing workflow:
1. Read a dwca/xml archive
2. Collect raw records into small batches (batch size is configurable)
3. Get or create GBIF id for each element of the batch and create keys (salt + ":" + GBIF id)
4. Get **dateCreated** from the table using GBIF id, if a record is exist
5. Create HBase put(create new or update existing) records and upload them into HBase
